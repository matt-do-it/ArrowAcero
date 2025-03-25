#include <iostream>
#include <algorithm>
#include <iterator>
#include <random>
#include <string>

#include <chrono>
#include <type_traits>

#include <arrow/api.h>

#include <arrow/compute/api.h>
#include <arrow/acero/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>

#include <arrow/visit_data_inline.h>

#include <boost/url.hpp>

namespace ac = arrow::acero;
namespace cp = arrow::compute;

class URLParseOptionsType : public cp::FunctionOptionsType {
    const char* type_name() const override { return "URLParseOptionsType"; }
    std::string Stringify(const cp::FunctionOptions&) const override {
        return "URLParseOptionsType";
    }
    bool Compare(const cp::FunctionOptions&, const cp::FunctionOptions&) const override {
        return true;
    }
    std::unique_ptr<cp::FunctionOptions> Copy(const cp::FunctionOptions&) const override;
};

cp::FunctionOptionsType* GetURLParseOptionsType();

enum URLParseOptionsExtract {
  HOST,
  PATH
};

class URLParseOptions : public cp::FunctionOptions {
    
public:
    URLParseOptionsExtract extract = HOST;
    
    URLParseOptions(URLParseOptionsExtract _extract = HOST) :
        cp::FunctionOptions(GetURLParseOptionsType()) {
        extract = _extract;
    }
};

void RegisterCustomFunctions();
