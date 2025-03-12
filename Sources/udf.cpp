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
    virtual arrow::Status PreExec(cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult* out) {
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


std::unique_ptr<cp::FunctionOptions> URLParseOptionsType::Copy(
    const cp::FunctionOptions&) const {
  return std::make_unique<URLParseOptions>();
}


void RegisterCustomFunctions() {
    auto func = std::make_shared<cp::ScalarFunction>("url_extract", cp::Arity::Unary(), func_doc);
    
    cp::ScalarKernel kernel({arrow::utf8()},
                            arrow::utf8(), StringTransformExec<arrow::StringType, URLParseTransform, URLParseOptions>::Execute, StringTransformExec<arrow::StringType, URLParseTransform, URLParseOptions>::State::Init);
    
    kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
    kernel.null_handling = cp::NullHandling::INTERSECTION;
    
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
    
    auto registry = cp::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));
    ARROW_RETURN_NOT_OK(registry->AddFunctionOptionsType(GetURLParseOptionsType()));
}

cp::FunctionOptionsType* GetURLParseOptionsType() {
  static URLParseOptionsType options_type;
  return &options_type;
}
