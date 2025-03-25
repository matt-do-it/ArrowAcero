//
//  udf.cpp
//  ArrowAcero
//
//  Created by Matt Herold on 12.03.25.
//
#import "udf.h"

template <typename offset_type> static int64_t GetVarBinaryValuesLength(const arrow::ArraySpan& span) {
    const offset_type* offsets = span.GetValues<offset_type>(1);
    return span.length > 0 ? offsets[span.length] - offsets[0] : 0;
}


template <typename OptionsType>
struct OptionsWrapper : public cp::KernelState {
    explicit OptionsWrapper(OptionsType options) : options(std::move(options)) {}
    
    static arrow::Result<std::unique_ptr<KernelState>> Init(cp::KernelContext* ctx,
                                                            const cp::KernelInitArgs& args) {
        if (auto options = static_cast<const OptionsType*>(args.options)) {
            return std::make_unique<OptionsWrapper>(*options);
        }
        
        return arrow::Status::Invalid(
                                      "Attempted to initialize KernelState from null FunctionOptions");
    }
    
    static const OptionsType& Get(const KernelState& state) {
        return ::arrow::internal::checked_cast<const OptionsWrapper&>(state).options;
    }
    
    static const OptionsType& Get(cp::KernelContext* ctx) { return Get(*ctx->state()); }
    
    OptionsType options;
};

struct StringTransformBase {
    virtual ~StringTransformBase() = default;
    virtual arrow::Status PreExec(cp::KernelContext* ctx,
                                  const cp::ExecSpan& batch,
                                  cp::ExecResult* out) {
        return arrow::Status::OK();
    }
    
    // Return the maximum total size of the output in codeunits (i.e. bytes)
    // given input characteristics.
    virtual int64_t MaxCodeunits(int64_t ninputs, int64_t input_ncodeunits) {
        return input_ncodeunits;
    }
    
    virtual arrow::Status InvalidInputSequence() {
        return arrow::Status::Invalid("Invalid UTF8 sequence in input");
    }
    
    int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                      uint8_t* output) {
        return 0;
    }
};

template <typename Type, typename StringTransform, typename Options> struct StringTransformExec {
    using State = OptionsWrapper<Options>;
    
    static arrow::Status Execute(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                                 cp::ExecResult* out) {
        const auto& options = State::Get(ctx);
        
        StringTransform transform;
        RETURN_NOT_OK(transform.PreExec(ctx, batch, out));
        // return x + y + z
        const arrow::ArraySpan& input = batch[0].array;
        
        auto offsets = input.GetValues<typename Type::offset_type>(1);
        const uint8_t* input_data = input.buffers[2].data;
        
        const int64_t input_ncodeunits = GetVarBinaryValuesLength<typename Type::offset_type>(input);
        const int64_t max_output_ncodeunits =
        transform.MaxCodeunits(input.length, input_ncodeunits);
        RETURN_NOT_OK(CheckOutputCapacity(max_output_ncodeunits));
        
        arrow::ArrayData* output = out->array_data().get();
        ARROW_ASSIGN_OR_RAISE(auto values_buffer, ctx->Allocate(max_output_ncodeunits));
        output->buffers[2] = values_buffer;
        
        // String offsets are preallocated
        typename Type::offset_type* output_string_offsets = output->GetMutableValues<typename Type::offset_type>(1);
        uint8_t* output_str = output->buffers[2]->mutable_data();
        
        typename Type::offset_type output_ncodeunits = 0;
        output_string_offsets[0] = output_ncodeunits;
        
        for (int64_t i = 0; i < input.length; i++) {
            if (!input.IsNull(i)) {
                const uint8_t* input_string = input_data + offsets[i];
                typename Type::offset_type input_string_ncodeunits = offsets[i + 1] - offsets[i];
                auto encoded_nbytes = static_cast<typename Type::offset_type>(transform.Transform(
                                                                                                  input_string, input_string_ncodeunits,         output_str + output_ncodeunits));
                
                if (encoded_nbytes < 0) {
                    return transform.InvalidInputSequence();
                }
                
                output_ncodeunits += encoded_nbytes;
            }
            output_string_offsets[i + 1] = output_ncodeunits;
        }
        
        return values_buffer->Resize(output_ncodeunits, /*shrink_to_fit=*/true);
    }
    
    static arrow::Status CheckOutputCapacity(int64_t ncodeunits) {
        if (ncodeunits > std::numeric_limits<typename Type::offset_type>::max()) {
            return arrow::Status::CapacityError(
                                                "Result might not fit in a 32bit utf8 array, convert to large_utf8");
        }
        return arrow::Status::OK();
    }
};


struct URLParseTransform : StringTransformBase {
    int64_t Transform(const uint8_t* input, int64_t input_string_ncodeunits,
                      uint8_t* output) {
        
        uint8_t* output_start = output;
        
        std::string_view s{(char*)input, (unsigned long)input_string_ncodeunits};
        
        boost::system::result<boost::url_view> r = boost::urls::parse_uri( s );
        if (r.has_value()) {
            boost::url_view u = r.value();
            
            std::string host = u.host();
            
            memcpy(output, host.data(), host.size());
            return host.size();
        } else {
            return 0;
        }
    }
};


const cp::FunctionDoc func_doc{
    "User-defined-function to parse URLs",
    "returns parse URL",
    {"x"},
    "URLParseOptions"};

template <typename Type> struct DictTransformExec {
    using BuilderType = typename arrow::TypeTraits<Type>::BuilderType;
    
    
    static arrow::Status Execute(cp::KernelContext* ctx, const cp::ExecSpan& batch,
                                 cp::ExecResult* out) {
        
        std::shared_ptr<arrow::DataType> type = out->array_data()->type;
        ARROW_ASSIGN_OR_RAISE(std::unique_ptr<arrow::ArrayBuilder> array_builder,
                              MakeBuilder(type, ctx->memory_pool()));
        
        arrow::StructBuilder* struct_builder = arrow::internal::checked_cast<arrow::StructBuilder*>(array_builder.get());
        
        ARROW_RETURN_NOT_OK(struct_builder->Reserve(batch[0].length()));
        std::cout << batch[0].length() << std::endl;
        std::vector<BuilderType*> field_builders;
        
        field_builders.reserve(13);
        for (int i = 0; i < 13; i++) {
            field_builders.push_back(
                                     dynamic_cast<BuilderType*>(struct_builder->field_builder(i)));
            RETURN_NOT_OK(field_builders.back()->Reserve(batch[0].length()));
        }
        
        auto visit_null = [&]() {
            return struct_builder->AppendNull();
        };
        
        auto visit_value = [&](std::string_view s) {
            boost::system::result<boost::url> r = boost::urls::parse_uri( s );
            if (r.has_value()) {
                boost::url u = r.value();
                
                // Normalization goes here
                u.normalize();
                
                if (u.has_scheme()) {
                    RETURN_NOT_OK(field_builders[0]->Append(u.scheme()));
                } else {
                    RETURN_NOT_OK(field_builders[0]->AppendNull());
                }

                RETURN_NOT_OK(field_builders[1]->Append(u.host()));
                RETURN_NOT_OK(field_builders[2]->Append(u.path()));

                if (u.has_query()) {
                    RETURN_NOT_OK(field_builders[3]->Append(u.query()));
                } else {
                    RETURN_NOT_OK(field_builders[3]->AppendNull());
                }

                if (u.has_fragment()) {
                    RETURN_NOT_OK(field_builders[4]->Append(u.fragment()));
                } else {
                    RETURN_NOT_OK(field_builders[4]->AppendNull());
                }

                RETURN_NOT_OK(field_builders[5]->Append(u.host() + u.path()));

                boost::urls::segments_view sv = u.segments();
                
                auto segmentIterator = sv.begin();
                    
                if (segmentIterator != sv.end()) {
                    RETURN_NOT_OK(field_builders[6]->Append("/" + (*segmentIterator)));
                    segmentIterator++;
                } else {
                    RETURN_NOT_OK(field_builders[6]->AppendNull());
                }
                                
                if (segmentIterator != sv.end()) {
                    RETURN_NOT_OK(field_builders[7]->Append("/" + (*segmentIterator)));
                    segmentIterator++;
                } else {
                    RETURN_NOT_OK(field_builders[7]->AppendNull());
                }
                
                if (segmentIterator != sv.end()) {
                    RETURN_NOT_OK(field_builders[8]->Append("/" + (*segmentIterator)));
                } else {
                    RETURN_NOT_OK(field_builders[8]->AppendNull());
                }
                
                boost::urls::params_view pv = u.params();
                
                auto utm_campaign = pv.find("utm_campaign");
                if (utm_campaign != pv.end()) {
                    RETURN_NOT_OK(field_builders[9]->Append((*utm_campaign).value));
                } else {
                    RETURN_NOT_OK(field_builders[9]->AppendNull());
                }

                auto utm_source = pv.find("utm_source");
                if (utm_source != pv.end()) {
                    RETURN_NOT_OK(field_builders[10]->Append((*utm_source).value));
                } else {
                    RETURN_NOT_OK(field_builders[10]->AppendNull());
                }

                auto utm_medium = pv.find("utm_medium");
                if (utm_medium != pv.end()) {
                    RETURN_NOT_OK(field_builders[11]->Append((*utm_medium).value));
                } else {
                    RETURN_NOT_OK(field_builders[11]->AppendNull());
                }

                auto utm_term = pv.find("utm_term");
                if (utm_term != pv.end()) {
                    RETURN_NOT_OK(field_builders[12]->Append((*utm_term).value));
                } else {
                    RETURN_NOT_OK(field_builders[12]->AppendNull());
                }


                return struct_builder->Append();
            } else {
                return struct_builder->AppendNull();
            }
        };
        
        RETURN_NOT_OK(arrow::VisitArraySpanInline<Type>(batch[0].array, visit_value, visit_null));
        
        std::shared_ptr<arrow::Array> out_array;
        RETURN_NOT_OK(struct_builder->Finish(&out_array));
        
        out->value = std::move(out_array->data());
        return arrow::Status::OK();
    }
    
};

const cp::FunctionDoc func_struct_doc{
    "User-defined-function to parse URLs",
    "returns parsed URL as components",
    {"x"},
    "URLParseOptions"};


std::unique_ptr<cp::FunctionOptions> URLParseOptionsType::Copy(
                                                               const cp::FunctionOptions&) const {
                                                                   return std::make_unique<URLParseOptions>();
                                                               }


void RegisterCustomFunctions() {
    auto func = std::make_shared<cp::ScalarFunction>("url_extract",
                                                     cp::Arity::Unary(),
                                                     func_doc);
    
    cp::ScalarKernel kernel({arrow::utf8()},
                            arrow::utf8(),
                            StringTransformExec<arrow::StringType, URLParseTransform, URLParseOptions>::Execute,StringTransformExec<arrow::StringType, URLParseTransform, URLParseOptions>::State::Init);
    
    kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
    kernel.null_handling = cp::NullHandling::INTERSECTION;
    
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
    
    auto registry = cp::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));
    ARROW_RETURN_NOT_OK(registry->AddFunctionOptionsType(GetURLParseOptionsType()));
    
    auto dict_func = std::make_shared<cp::ScalarFunction>("url_extract_dict",
                                                          cp::Arity::Unary(),
                                                          func_struct_doc);
    arrow::FieldVector fields;
    fields.reserve(13);
    
    fields.push_back(arrow::field("scheme", arrow::utf8()));
    
    fields.push_back(arrow::field("host", arrow::utf8()));
    fields.push_back(arrow::field("path", arrow::utf8()));
    fields.push_back(arrow::field("query", arrow::utf8()));
    fields.push_back(arrow::field("fragment", arrow::utf8()));
    
    fields.push_back(arrow::field("combinedPagePath", arrow::utf8()));
    
    fields.push_back(arrow::field("pagePath1", arrow::utf8()));
    fields.push_back(arrow::field("pagePath2", arrow::utf8()));
    fields.push_back(arrow::field("pagePath3", arrow::utf8()));

    fields.push_back(arrow::field("utm_campaign", arrow::utf8()));
    fields.push_back(arrow::field("utm_source", arrow::utf8()));
    fields.push_back(arrow::field("utm_medium", arrow::utf8()));
    fields.push_back(arrow::field("utm_term", arrow::utf8()));

    cp::ScalarKernel struct_kernel({arrow::utf8()},
                                   struct_(std::move(fields)),
                                   DictTransformExec<arrow::StringType>::Execute);
    
    struct_kernel.null_handling = cp::NullHandling::COMPUTED_NO_PREALLOCATE;
    struct_kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
    
    ARROW_RETURN_NOT_OK(dict_func->AddKernel(std::move(struct_kernel)));
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(dict_func)));
}

cp::FunctionOptionsType* GetURLParseOptionsType() {
    static URLParseOptionsType options_type;
    return &options_type;
}
