#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const
    {
        char input[256];
        std::string result;

        std::string command = "printf '" + content + "' | sha256sum";

        FILE *f = popen(command.c_str(), "r");

        while (!feof(f)) {
            if (fgets(input, 256, f) != NULL) {
                result += input;
            }
        }

        pclose(f);
        result = result.substr(0, result.length() - 4);
        return PageId(result);
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
