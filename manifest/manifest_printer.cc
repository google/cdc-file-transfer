// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "manifest/manifest_printer.h"

#include "manifest/content_id.h"
#include "manifest/manifest_proto_defs.h"

namespace cdc_ft {

// A special text proto printer that prints all ContentId protos using a
// hexadecimal representation instead of octal-escaped string values.
class ContentIdPrinter : public google::protobuf::TextFormat::MessagePrinter {
 public:
  ContentIdPrinter() = default;
  virtual ~ContentIdPrinter() = default;
  void Print(const google::protobuf::Message& message, bool single_line_mode,
             google::protobuf::TextFormat::BaseTextGenerator* generator)
      const override {
    const ContentIdProto* content_id =
        dynamic_cast<const ContentIdProto*>(&message);
    if (content_id) {
      generator->PrintLiteral("blake3_sum_160: \"");
      generator->PrintString(ContentId::ToHexString(*content_id));
      generator->PrintLiteral("\"");
    } else {
      // Technically, we should just call the inherited Print() function, but
      // this results in a linker error for unknown reasons. But since we are
      // never supposed to be called for any other message type, let's not
      // bother.
      generator->PrintLiteral("(given message is no ContentId proto)");
    }
    if (!single_line_mode) generator->PrintLiteral("\n");
  }
};

ManifestPrinter::ManifestPrinter() {
  ContentIdPrinter* printer = new ContentIdPrinter;
  // If registration of a printer is successful, the callee takes ownership of
  // the object.
  if (!RegisterMessagePrinter(ContentIdProto::default_instance().descriptor(),
                              printer)) {
    // Registration unsuccessful, delete the object.
    delete printer;
  }
}

}  // namespace cdc_ft
