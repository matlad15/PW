#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::mutex dangleSumMutex;
        std::mutex differenceMutex;

        double difference = 0.0;
        double dangleSum = 0.0;
        uint32_t numThr = numThreads;

        std::vector<PageIdAndRank> result;

        std::thread t[numThreads];
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::vector<PageId> danglingNodes;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;

        std::vector<Page> pageVec = network.getPages();

        auto preprocessing = [&network, &pageVec, &numThr](uint32_t num) {
            uint32_t cnst = (pageVec.size()  + numThr - 1) / numThr;
            uint32_t beg = num * cnst;
            uint32_t end = std::min((num + 1) * cnst, (uint32_t)pageVec.size());
            for (auto i = beg; i < end; i++) {
                pageVec[i].generateId(network.getGenerator());
            }
        };

        auto dangleSumCompute = [&previousPageHashMap, &danglingNodes, &dangleSum, &numThr, &dangleSumMutex](uint32_t num) {
            uint32_t cnst = (danglingNodes.size() + numThr - 1) / numThr;
            uint32_t beg = num * cnst;
            uint32_t end = std::min((num + 1) * cnst, (uint32_t)danglingNodes.size());
            double helper = 0.0;
            for (uint32_t k = beg; k < end; ++k) {
                auto danglingNode = danglingNodes[k];
                helper += previousPageHashMap[danglingNode];
            }
            dangleSumMutex.lock();
            dangleSum += helper;
            dangleSumMutex.unlock();
        };

        auto rankCompute = [&network, &pageVec, &pageHashMap, &previousPageHashMap, &numLinks, &edges, &difference,
                                &dangleSum, &numThr, &alpha, &differenceMutex](uint32_t num) {
            double helper = 0.0;
            uint32_t cnst = (pageVec.size()  + numThr - 1) / numThr;
            uint32_t beg = num * cnst;
            uint32_t end = std::min((num + 1) * cnst, (uint32_t)pageVec.size());
            for (auto k = beg; k < end; ++k) {
                auto pageId = pageVec[k].getId();
                double pageMapElem = 0.0;
                double danglingWeight = 1.0 / network.getSize();
                pageMapElem = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        pageMapElem += alpha * previousPageHashMap[link] / numLinks[link];
                    }
                }
                pageHashMap[pageId] = pageMapElem;
                helper += std::abs(previousPageHashMap[pageId] - pageHashMap[pageId]);
            }

            differenceMutex.lock();
            difference += helper;
            differenceMutex.unlock();
        };

        for (uint32_t i = 0; i < numThreads; ++i) {
            t[i] = std::thread{preprocessing, i};
        }

        for (uint32_t i = 0; i < numThreads; ++i) {
            t[i].join();
        }

        for (auto const& page : pageVec) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }

        for (auto  page : pageVec) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        for (auto page : pageVec) {
            if (page.getLinks().size() == 0) {
                danglingNodes.push_back(page.getId());
            }
        }

        for (auto page : pageVec) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        for (uint32_t j = 0; j < iterations; ++j) {
            
            previousPageHashMap = pageHashMap;
   
            for (uint32_t i = 0; i < numThreads; ++i) {
                t[i] = std::thread{dangleSumCompute, i};
            }

            for (uint32_t i = 0; i < numThreads; ++i) {
                t[i].join();
            }

            dangleSum = dangleSum * alpha;

            for (uint32_t i = 0; i < numThreads; ++i) {
                t[i] = std::thread{rankCompute, i};
            }

            for (uint32_t i = 0; i < numThreads; ++i) {
                t[i].join();
            }

            if (difference < tolerance) {
                for (auto iter : pageHashMap) {
                    result.push_back(PageIdAndRank(iter.first, iter.second));
                }

                ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);

                return result;
            }

            difference = 0;
            dangleSum = 0;
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
