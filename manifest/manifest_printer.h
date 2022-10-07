/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MANIFEST_MANIFEST_PRINTER_H_
#define MANIFEST_MANIFEST_PRINTER_H_

#include <google/protobuf/text_format.h>

namespace cdc_ft {

// This class prints manifest protos as text, but uses a hexadecimal
// representation for all ContentId protos to make them human-readable.
//
// Usage:
//   AssetListProto pb;
//   // ...
//   ManifestPrinter printer;
//   std::string s;
//   printer.PrintToString(pb, s);
//   std::cout << s << std::endl;
class ManifestPrinter : public google::protobuf::TextFormat::Printer {
 public:
  ManifestPrinter();
  virtual ~ManifestPrinter() = default;
};

}  // namespace cdc_ft

#endif  // MANIFEST_MANIFEST_PRINTER_H_
