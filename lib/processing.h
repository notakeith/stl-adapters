#pragma once
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace fs = std::filesystem;

template <typename T>
class DataStream {
   public:
    using value_type = T;
    virtual ~DataStream() = default;

    class Iterator {
       public:
        using value_type = T;
        using reference = T&;
        using pointer = T*;
        using iterator_category = std::input_iterator_tag;

        virtual ~Iterator() = default;
        virtual T operator*() const = 0;
        virtual Iterator& operator++() = 0;
        virtual bool operator!=(const Iterator& other) const = 0;
        virtual bool operator==(const Iterator& other) const = 0;

        virtual bool getIsEnd() const = 0;
    };

    virtual std::unique_ptr<Iterator> begin_ptr() = 0;
    virtual std::unique_ptr<Iterator> end_ptr() = 0;

    class IteratorWrapper {
       public:
        IteratorWrapper(std::unique_ptr<Iterator>&& it)
            : it(std::move(it)) {}

        T operator*() const { return **it; }
        IteratorWrapper& operator++() {
            ++*it;
            return *this;
        }
        bool operator!=(const IteratorWrapper& other) const {
            return *it != *other.it;
        }

        bool getIsEnd() const {
            return it->getIsEnd();
        }

       private:
        std::unique_ptr<Iterator> it;
    };

    virtual IteratorWrapper begin() { return IteratorWrapper(begin_ptr()); }
    virtual IteratorWrapper end() { return IteratorWrapper(end_ptr()); }
};

class DirStream : public DataStream<fs::path> {
   public:
    class DirIterator : public Iterator {
       public:
        DirIterator()
            : is_end(true) {}

        DirIterator(fs::path path, bool recursive)
            : recursive(recursive), is_end(false) {
            if (recursive) {
                iterator.emplace<fs::recursive_directory_iterator>(path);
            } else {
                iterator.emplace<fs::directory_iterator>(path);
            }
            skip_non_regular_files();
        }

        fs::path operator*() const override {
            return std::visit([](auto& it) -> fs::path { return *it; }, iterator);
        }

        Iterator& operator++() override {
            std::visit([](auto& it) { ++it; }, iterator);
            skip_non_regular_files();
            return *this;
        }

        bool operator!=(const Iterator& other) const override {
            const DirIterator* other_ptr = dynamic_cast<const DirIterator*>(&other);
            if (!other_ptr) return true;

            if (is_end && other_ptr->is_end) return false;
            if (is_end != other_ptr->is_end) return true;

            return iterator.index() != other_ptr->iterator.index();
        }

        bool operator==(const Iterator& other) const override {
            return !operator!=(other);
        }

        bool getIsEnd() const override {
            return is_end;
        }

       private:
        void skip_non_regular_files() {
            bool done = false;
            while (!done) {
                std::visit([this, &done](auto& it) {
                    if (it == fs::end(it)) {
                        done = true;
                        is_end = true;
                    } else if (fs::is_regular_file(it->path())) {
                        done = true;
                    } else {
                        ++it;
                    }
                },
                           iterator);
            }
        }

        std::variant<fs::directory_iterator, fs::recursive_directory_iterator> iterator;
        bool recursive;
        bool is_end;
    };

    DirStream(const std::string& path, bool recursive)
        : path(path), recursive(recursive) {
        if (!fs::exists(path)) {
            throw std::runtime_error("Path does not exist: " + path);
        }
    }

    std::unique_ptr<Iterator> begin_ptr() override {
        return std::unique_ptr<Iterator>(new DirIterator(path, recursive));
    }

    std::unique_ptr<Iterator> end_ptr() override {
        return std::unique_ptr<Iterator>(new DirIterator());
    }

   private:
    fs::path path;
    bool recursive;
};

class FileContentStream : public DataStream<std::string> {
   public:
    FileContentStream(std::unique_ptr<DataStream<fs::path>> path_stream)
        : path_stream(std::move(path_stream)) {}

    class FileIterator : public Iterator {
       public:
        FileIterator(std::unique_ptr<DataStream<fs::path>::Iterator> path_it,
                     std::unique_ptr<DataStream<fs::path>::Iterator> path_end)
            : path_it(std::move(path_it)),
              path_end(std::move(path_end)),
              current_file(nullptr),
              is_end(false) {
            open_next_file();
        }

        FileIterator(std::unique_ptr<DataStream<fs::path>::Iterator>&& path_it)
            : path_it(std::move(path_it)),
              is_end(!this->path_it || *this->path_it == *path_end) {
            if (this->path_it) {
                open_next_file();
            }
        }

        std::string operator*() const override {
            if (is_end || !current_file || !*current_file) {
                throw std::runtime_error("Attempted to dereference invalid file iterator");
            }
            return current_line.empty() ? "" : current_line;
            ;
        }

        Iterator& operator++() override {
            if (is_end) return *this;

            if (std::getline(*current_file, current_line)) {
                return *this;
            }
            current_file.reset();
            open_next_file();
            return *this;
        }

        bool operator!=(const Iterator& other) const override {
            const FileIterator* other_ptr = static_cast<const FileIterator*>(&other);

            if(!other_ptr) {
                return true;
            }

            if (is_end && other_ptr->is_end) {
                return false;
            }

            if (path_it != other_ptr->path_it) {
                return true;
            }

            return current_file->tellg() != other_ptr->current_file->tellg();
        }

        bool operator==(const Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return is_end;
        }

       private:
        void open_next_file() {
            current_line.clear();

            while (!path_it->getIsEnd() && *path_it != *path_end) {
                fs::path file_path = **path_it;
                current_file = std::make_unique<std::ifstream>(file_path);
                if (current_file && *current_file) {
                    ++*path_it;
                    is_end = false;

                    if (std::getline(*current_file, current_line)) {
                        if (!current_line.empty()) {
                            return;
                        }
                    }
                }
                current_file.reset();
                ++*path_it;
            }

            current_file.reset();
            is_end = true;
        }

        std::unique_ptr<DataStream<fs::path>::Iterator> path_it;
        std::unique_ptr<DataStream<fs::path>::Iterator> path_end;
        std::unique_ptr<std::ifstream> current_file;
        std::string current_line;
        bool is_end;
    };

    std::unique_ptr<Iterator> begin_ptr() override {
        return std::unique_ptr<Iterator>(new FileIterator(path_stream->begin_ptr()));
    }

    std::unique_ptr<Iterator> end_ptr() override {
        return std::unique_ptr<Iterator>(new FileIterator(nullptr));
    }

   private:
    std::unique_ptr<DataStream<fs::path>> path_stream;
};

template <typename T>
class VectorDataStream : public DataStream<T> {
   public:
    class VectorIterator : public DataStream<T>::Iterator {
       public:
        VectorIterator(const std::vector<T>& data)
            : data(&data), index(0), is_end(false) {}

        VectorIterator()
            : is_end(true) {}

        T operator*() const override {
            return (*data)[index];
        }

        typename DataStream<T>::Iterator& operator++() override {
            ++index;
            if (data && index >= data->size()) {
                is_end = true;
            }
            return *this;
        }

        bool operator!=(const typename DataStream<T>::Iterator& other) const override {
            const VectorIterator* other_ptr = dynamic_cast<const VectorIterator*>(&other);
            if (!other_ptr) return true;
            return !is_end || !other_ptr->is_end;
        }

        bool operator==(const typename DataStream<T>::Iterator& other) const override {
            return operator!=(other);
        }

        bool getIsEnd() const override {
            return is_end;
        }

       private:
        const std::vector<T>* data;
        size_t index;
        bool is_end;
    };

    void add(const T& value) {
        data.push_back(value);
    }

    VectorDataStream() = default;

    VectorDataStream(const std::vector<T>& data)
        : data(data) {}

    VectorDataStream(std::vector<T>&& data)
        : data(std::move(data)) {}

    std::unique_ptr<typename DataStream<T>::Iterator> begin_ptr() override {
        return std::make_unique<VectorIterator>(data);
    }

    std::unique_ptr<typename DataStream<T>::Iterator> end_ptr() override {
        return std::make_unique<VectorIterator>();
    }

   private:
    std::vector<T> data;
};

template <typename T>
class FilteredDataStream : public DataStream<T> {
   public:
    class FilteredIterator : public DataStream<T>::Iterator {
       public:
        template <typename Predicate>
        FilteredIterator(typename DataStream<T>::IteratorWrapper it,
                         typename DataStream<T>::IteratorWrapper end,
                         Predicate&& predicate)
            : it(std::move(it)), end(std::move(end)), predicate(std::forward<Predicate>(predicate)) {
            skipInvalid();
        }

        T operator*() const override {
            return *it;
        }

        typename DataStream<T>::Iterator& operator++() override {
            ++it;
            skipInvalid();
            return *this;
        }

        bool operator!=(const typename DataStream<T>::Iterator& other) const override {
            const FilteredIterator* other_ptr = dynamic_cast<const FilteredIterator*>(&other);
            if (!other_ptr) return true;
            return it != other_ptr->it;
        }

        bool operator==(const typename DataStream<T>::Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return it.getIsEnd();
        }

       private:
        void skipInvalid() {
            while (!it.getIsEnd() && !predicate(*it)) {
                ++it;
            }
        }

        typename DataStream<T>::IteratorWrapper it;
        typename DataStream<T>::IteratorWrapper end;
        std::function<bool(const T&)> predicate;
    };

    template <typename Predicate>
    FilteredDataStream(std::unique_ptr<DataStream<T>> stream, Predicate&& predicate)
        : stream(std::move(stream)), predicate(std::forward<Predicate>(predicate)) {}

    std::unique_ptr<typename DataStream<T>::Iterator> begin_ptr() override {
        return std::unique_ptr<typename DataStream<T>::Iterator>(
            new FilteredIterator(stream->begin(), stream->end(), predicate));
    }

    std::unique_ptr<typename DataStream<T>::Iterator> end_ptr() override {
        return std::unique_ptr<typename DataStream<T>::Iterator>(
            new FilteredIterator(stream->end(), stream->end(), predicate));
    }

   private:
    std::unique_ptr<DataStream<T>> stream;
    std::function<bool(const T&)> predicate;
};

template <typename T>
class TransformedDataStream : public DataStream<T> {
   public:
    class TransformedIterator : public DataStream<T>::Iterator {
       public:
        template <typename Func>
        TransformedIterator(typename DataStream<T>::IteratorWrapper it,
                            typename DataStream<T>::IteratorWrapper end,
                            Func&& transform_func)
            : it(std::move(it)), end(std::move(end)), transform_func(std::forward<Func>(transform_func)) {}

        T operator*() const override {
            return transform_func(*it);
        }

        typename DataStream<T>::Iterator& operator++() override {
            ++it;
            return *this;
        }

        bool operator!=(const typename DataStream<T>::Iterator& other) const override {
            const TransformedIterator* other_ptr = dynamic_cast<const TransformedIterator*>(&other);
            if (!other_ptr) return true;
            return it != other_ptr->it;
        }

        bool operator==(const typename DataStream<T>::Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return it.getIsEnd();
        }

       private:
        typename DataStream<T>::IteratorWrapper it;
        typename DataStream<T>::IteratorWrapper end;
        std::function<T(const T&)> transform_func;
    };

    template <typename Func>
    TransformedDataStream(std::unique_ptr<DataStream<T>> stream, Func&& transform_func)
        : stream(std::move(stream)), transform_func(std::forward<Func>(transform_func)) {}

    std::unique_ptr<typename DataStream<T>::Iterator> begin_ptr() override {
        return std::make_unique<TransformedIterator>(stream->begin(), stream->end(), transform_func);
    }

    std::unique_ptr<typename DataStream<T>::Iterator> end_ptr() override {
        return std::make_unique<TransformedIterator>(stream->end(), stream->end(), transform_func);
    }

   private:
    std::unique_ptr<DataStream<T>> stream;
    std::function<T(const T&)> transform_func;
};

template <typename T>
class SplitDataStream : public DataStream<std::string> {
   public:
    class SplitIterator : public DataStream<std::string>::Iterator {
       public:
        SplitIterator(typename DataStream<T>::IteratorWrapper it,
                      typename DataStream<T>::IteratorWrapper end,
                      const std::unordered_set<char>& delimiters)
            : it(std::move(it)), end(std::move(end)), delimiters_(delimiters) {
            skipToNext();
        }

        std::string operator*() const override {
            return current_line;
        }

        typename DataStream<std::string>::Iterator& operator++() override {
            skipToNext();
            return *this;
        }

        bool operator!=(const typename DataStream<std::string>::Iterator& other) const override {
            const SplitIterator* other_ptr = dynamic_cast<const SplitIterator*>(&other);
            if (!other_ptr) return true;
            return it != other_ptr->it || !current_line.empty();
        }

        bool operator==(const typename DataStream<std::string>::Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return it.getIsEnd() && current_line.empty();
        }

       private:
        void skipToNext() {
            current_line.clear();
            while (!it.getIsEnd()) {
                const std::string& line = *it;

                for (; pos_in_line < line.size(); ++pos_in_line) {
                    char c = line[pos_in_line];
                    if (delimiters_.count(c)) {
                        if (!current_line.empty()) {
                            ++pos_in_line;
                            return;
                        }
                    } else {
                        current_line += c;
                    }
                }

                ++it;
                pos_in_line = 0;

                if (!current_line.empty()) {
                    return;
                }
            }
        }

        typename DataStream<T>::IteratorWrapper it;
        typename DataStream<T>::IteratorWrapper end;
        std::unordered_set<char> delimiters_;
        std::string current_line;
        size_t pos_in_line = 0;
    };

    template <typename U>
    SplitDataStream(std::unique_ptr<DataStream<U>> stream, const std::string& filter_string)
        : stream(std::move(stream)), delimiters_(filter_string.begin(), filter_string.end()) {}

    std::unique_ptr<typename DataStream<std::string>::Iterator> begin_ptr() override {
        return std::make_unique<SplitIterator>(stream->begin(), stream->end(), delimiters_);
    }

    std::unique_ptr<typename DataStream<std::string>::Iterator> end_ptr() override {
        return std::make_unique<SplitIterator>(stream->end(), stream->end(), delimiters_);
    }

   private:
    std::unique_ptr<DataStream<T>> stream;
    std::unordered_set<char> delimiters_;
};

template <typename T, typename Accumulator, typename AccumulateFunc, typename KeyFunc>
class AggregateByKeyDataStream : public DataStream<std::pair<decltype(KeyFunc()(std::declval<T>())), Accumulator>> {
   public:
    using KeyType = decltype(KeyFunc()(std::declval<T>()));
    using ResultType = std::pair<KeyType, Accumulator>;

    class Iterator : public DataStream<ResultType>::Iterator {
       public:
        Iterator(
            std::vector<std::pair<KeyType, Accumulator>> results,
            size_t index = 0)
            : results_(std::move(results)), index_(index) {}

        ResultType operator*() const override {
            return results_[index_];
        }

        typename DataStream<ResultType>::Iterator& operator++() override {
            ++index_;
            return *this;
        }

        bool operator!=(const typename DataStream<ResultType>::Iterator& other) const override {
            const Iterator* other_ptr = dynamic_cast<const Iterator*>(&other);
            if (!other_ptr) return true;
            return index_ != other_ptr->index_;
        }

        bool operator==(const typename DataStream<ResultType>::Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return index_ >= results_.size();
        }

       private:
        std::vector<std::pair<KeyType, Accumulator>> results_;
        size_t index_;
    };

    AggregateByKeyDataStream(
        std::unique_ptr<DataStream<T>> stream,
        Accumulator initial_value,
        AccumulateFunc accumulate_func,
        KeyFunc key_func) {
        std::unordered_map<KeyType, Accumulator> aggregation_map;
        std::vector<KeyType> key_order;

        auto it = stream->begin();
        auto end = stream->end();

        while (!it.getIsEnd()) {
            const auto& value = *it;
            KeyType key = key_func(value);

            if (aggregation_map.find(key) == aggregation_map.end()) {
                aggregation_map[key] = initial_value;
                key_order.push_back(key);
            }

            accumulate_func(value, aggregation_map[key]);
            ++it;
        }

        for (const auto& key : key_order) {
            results_.emplace_back(key, aggregation_map[key]);
        }
    }

    std::unique_ptr<typename DataStream<ResultType>::Iterator> begin_ptr() override {
        return std::make_unique<Iterator>(results_, 0);
    }

    std::unique_ptr<typename DataStream<ResultType>::Iterator> end_ptr() override {
        return std::make_unique<Iterator>(results_, results_.size());
    }

   private:
    std::vector<std::pair<KeyType, Accumulator>> results_;
};

template <typename T, typename E>
class SplitExpectedDataStream {
   public:
    using ExpectedType = T;
    using UnexpectedType = E;

    struct Result {
        std::unique_ptr<DataStream<UnexpectedType>> unexpected_stream;
        std::unique_ptr<DataStream<ExpectedType>> expected_stream;
    };

    static Result split(std::unique_ptr<DataStream<std::expected<ExpectedType, UnexpectedType>>> stream) {
        auto unexpected_stream = std::make_unique<VectorDataStream<UnexpectedType>>();
        auto expected_stream = std::make_unique<VectorDataStream<ExpectedType>>();

        auto it = stream->begin();
        auto end = stream->end();

        while (!it.getIsEnd()) {
            const auto& value = *it;
            if (value.has_value()) {
                expected_stream->add(*value);
            } else {
                unexpected_stream->add(value.error());
            }
            ++it;
        }

        return {std::move(unexpected_stream), std::move(expected_stream)};
    }
};

template <typename LeftValue, typename RightValue>
struct JoinResult {
    LeftValue left_value;
    std::optional<RightValue> right_value;

    bool operator==(const JoinResult& other) const {
        return left_value == other.left_value && right_value == other.right_value;
    }
};

template <typename Left, typename Right>
class JoinedDataStream : public DataStream<JoinResult<Left, Right>> {
   public:
    using ResultType = JoinResult<Left, Right>;

    class Iterator : public DataStream<ResultType>::Iterator {
       public:
        Iterator(typename DataStream<Left>::IteratorWrapper left_it,
                 typename DataStream<Left>::IteratorWrapper left_end,
                 std::unordered_map<typename Left::key_type, std::vector<Right>> right_map)
            : left_it(std::move(left_it)),
              left_end(std::move(left_end)),
              right_map(std::move(right_map)) {
            advanceToNextValid();
        }

        ResultType operator*() const override {
            if (current_rights.empty()) {
                return {*left_it, std::nullopt};
            }
            return {*left_it, current_rights[current_right_index]};
        }

        typename DataStream<ResultType>::Iterator& operator++() override {
            if (!current_rights.empty()) {
                ++current_right_index;
                if (current_right_index < current_rights.size()) {
                    return *this;
                }
            }
            ++left_it;
            current_right_index = 0;
            advanceToNextValid();
            return *this;
        }

        bool operator!=(const typename DataStream<ResultType>::Iterator& other) const override {
            const Iterator* other_ptr = dynamic_cast<const Iterator*>(&other);
            if (!other_ptr) return true;
            return left_it != other_ptr->left_it ||
                   current_right_index != other_ptr->current_right_index;
        }

        bool operator==(const typename DataStream<ResultType>::Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return left_it.getIsEnd();
        }

       private:
        void advanceToNextValid() {
            while (!left_it.getIsEnd()) {
                auto key = (*left_it).key;
                auto it = right_map.find(key);
                if (it != right_map.end()) {
                    current_rights = it->second;
                    return;
                }
                current_rights.clear();
                return;
            }
        }

        typename DataStream<Left>::IteratorWrapper left_it;
        typename DataStream<Left>::IteratorWrapper left_end;
        std::unordered_map<typename Left::key_type, std::vector<Right>> right_map;
        std::vector<Right> current_rights;
        size_t current_right_index = 0;
    };

    JoinedDataStream(std::unique_ptr<DataStream<Left>> left,
                     std::unique_ptr<DataStream<Right>> right)
        : left_stream(std::move(left)), right_stream(std::move(right)) {
        buildRightMap();
    }

    std::unique_ptr<typename DataStream<ResultType>::Iterator> begin_ptr() override {
        return std::make_unique<Iterator>(left_stream->begin(), left_stream->end(), right_map);
    }

    std::unique_ptr<typename DataStream<ResultType>::Iterator> end_ptr() override {
        return std::make_unique<Iterator>(left_stream->end(), left_stream->end(), right_map);
    }

   private:
    void buildRightMap() {
        auto it = right_stream->begin();
        auto end = right_stream->end();

        while (!it.getIsEnd()) {
            const auto& kv = *it;
            right_map[kv.key].push_back(kv.value);
            ++it;
        }
    }

    std::unique_ptr<DataStream<Left>> left_stream;
    std::unique_ptr<DataStream<Right>> right_stream;
    std::unordered_map<typename Left::key_type, std::vector<Right>> right_map;
};

template <typename Left, typename Right, typename LeftKeyFunc, typename RightKeyFunc>
class JoinedWithKeyDataStream : public DataStream<JoinResult<Left, Right>> {
   public:
    using ResultType = JoinResult<Left, Right>;
    using KeyType = decltype(LeftKeyFunc()(std::declval<Left>()));

    class Iterator : public DataStream<ResultType>::Iterator {
       public:
        Iterator(typename DataStream<Left>::IteratorWrapper left_it,
                 typename DataStream<Left>::IteratorWrapper left_end,
                 std::unordered_map<KeyType, std::vector<Right>> right_map,
                 LeftKeyFunc left_key_func)
            : left_it(std::move(left_it)),
              left_end(std::move(left_end)),
              right_map(std::move(right_map)),
              left_key_func(std::move(left_key_func)) {
            advanceToNextValid();
        }

        ResultType operator*() const override {
            if (current_rights.empty()) {
                return {*left_it, std::nullopt};
            }
            return {*left_it, current_rights[current_right_index]};
        }

        typename DataStream<ResultType>::Iterator& operator++() override {
            if (!current_rights.empty()) {
                ++current_right_index;
                if (current_right_index < current_rights.size()) {
                    return *this;
                }
            }
            ++left_it;
            current_right_index = 0;
            advanceToNextValid();
            return *this;
        }

        bool operator!=(const typename DataStream<ResultType>::Iterator& other) const override {
            const Iterator* other_ptr = dynamic_cast<const Iterator*>(&other);
            if (!other_ptr) return true;
            return left_it != other_ptr->left_it ||
                   current_right_index != other_ptr->current_right_index;
        }

        bool operator==(const typename DataStream<ResultType>::Iterator& other) const override {
            return !(*this != other);
        }

        bool getIsEnd() const override {
            return left_it.getIsEnd();
        }

       private:
        void advanceToNextValid() {
            while (!left_it.getIsEnd()) {
                auto key = left_key_func(*left_it);
                auto it = right_map.find(key);
                if (it != right_map.end()) {
                    current_rights = it->second;
                    return;
                }
                current_rights.clear();
                return;
            }
        }

        typename DataStream<Left>::IteratorWrapper left_it;
        typename DataStream<Left>::IteratorWrapper left_end;
        std::unordered_map<KeyType, std::vector<Right>> right_map;
        LeftKeyFunc left_key_func;
        std::vector<Right> current_rights;
        size_t current_right_index = 0;
    };

    JoinedWithKeyDataStream(std::unique_ptr<DataStream<Left>> left,
                            std::unique_ptr<DataStream<Right>> right,
                            LeftKeyFunc left_key_func,
                            RightKeyFunc right_key_func)
        : left_stream(std::move(left)),
          right_stream(std::move(right)),
          left_key_func(std::move(left_key_func)),
          right_key_func(std::move(right_key_func)) {
        buildRightMap();
    }

    std::unique_ptr<typename DataStream<ResultType>::Iterator> begin_ptr() override {
        return std::make_unique<Iterator>(left_stream->begin(), left_stream->end(), right_map, left_key_func);
    }

    std::unique_ptr<typename DataStream<ResultType>::Iterator> end_ptr() override {
        return std::make_unique<Iterator>(left_stream->end(), left_stream->end(), right_map, left_key_func);
    }

   private:
    void buildRightMap() {
        auto it = right_stream->begin();
        auto end = right_stream->end();

        while (!it.getIsEnd()) {
            const auto& value = *it;
            right_map[right_key_func(value)].push_back(value);
            ++it;
        }
    }

    std::unique_ptr<DataStream<Left>> left_stream;
    std::unique_ptr<DataStream<Right>> right_stream;
    LeftKeyFunc left_key_func;
    RightKeyFunc right_key_func;
    std::unordered_map<KeyType, std::vector<Right>> right_map;
};

class OpenFiles {
   public:
    std::unique_ptr<DataStream<std::string>> operator()(std::unique_ptr<DataStream<fs::path>> stream) const {
        return std::make_unique<FileContentStream>(std::move(stream));
    }
};

class Out {
   public:
    Out(std::ostream& os)
        : os(os) {}

    template <typename T>
    void operator()(std::unique_ptr<DataStream<T>> stream) const {
        auto it = stream->begin();
        auto end = stream->end();

        while (it != end) {
            os << *it << "\n";
            ++it;
        }
    }

   private:
    std::ostream& os;
};

class AsVector {
   public:
    template <typename T>
    std::vector<T> operator()(std::unique_ptr<DataStream<T>> stream) const {
        std::vector<T> result;
        auto it = stream->begin();
        auto end = stream->end();

        while (it != end) {
            result.push_back(*it);
            ++it;
        }

        return result;
    }
};

class Write {
   public:
    Write(std::ostream& output, char delimiter)
        : output(output), delimiter(delimiter) {}

    template <typename T>
    void operator()(std::unique_ptr<DataStream<T>> stream) const {
        auto it = stream->begin();
        auto end = stream->end();

        while (it != end) {
            output << *it << delimiter;
            ++it;
        }
    }

   private:
    std::ostream& output;
    char delimiter;
};

template <typename Predicate>
class Filter {
   public:
    Filter(Predicate&& predicate)
        : predicate_(std::forward<Predicate>(predicate)) {}

    template <typename T>
    std::unique_ptr<DataStream<T>> operator()(std::unique_ptr<DataStream<T>> stream) const {
        return std::make_unique<FilteredDataStream<T>>(std::move(stream), predicate_);
    }

   private:
    Predicate predicate_;
};

class DropNullopt {
   public:
    template <typename T>
    auto operator()(std::unique_ptr<DataStream<std::optional<T>>> stream) const {
        return Filter<std::function<bool(const std::optional<T>&)>>([](const std::optional<T>& value) -> bool { return value.has_value(); })(std::move(stream));
    }
};

template <typename Func>
class Transform {
   public:
    Transform(Func&& transform_func)
        : transform_func_(std::forward<Func>(transform_func)) {}

    template <typename T>
    std::unique_ptr<DataStream<T>> operator()(std::unique_ptr<DataStream<T>> stream) const {
        return std::make_unique<TransformedDataStream<T>>(std::move(stream), transform_func_);
    }

   private:
    Func transform_func_;
};

class Split {
   public:
    Split(const std::string& filter_string)
        : filter_string_(filter_string) {}

    template <typename T>
    std::unique_ptr<DataStream<std::string>> operator()(std::unique_ptr<DataStream<T>> stream) const {
        return std::make_unique<SplitDataStream<T>>(std::move(stream), filter_string_);
    }

   private:
    std::string filter_string_;
};

template <typename Accumulator, typename AccumulateFunc, typename KeyFunc>
class AggregateByKey {
   public:
    AggregateByKey(Accumulator initial_value, AccumulateFunc accumulate_func, KeyFunc key_func)
        : initial_value_(std::move(initial_value)),
          accumulate_func_(std::move(accumulate_func)),
          key_func_(std::move(key_func)) {}

    template <typename T>
    auto operator()(std::unique_ptr<DataStream<T>> stream) const {
        using ResultType = std::pair<decltype(key_func_(std::declval<T>())), Accumulator>;
        return std::unique_ptr<DataStream<ResultType>>(
            new AggregateByKeyDataStream<T, Accumulator, AccumulateFunc, KeyFunc>(
                std::move(stream),
                initial_value_,
                accumulate_func_,
                key_func_));
    }

   private:
    Accumulator initial_value_;
    AccumulateFunc accumulate_func_;
    KeyFunc key_func_;
};

template <typename T, typename E>
class SplitExpectedResult {
   public:
    std::unique_ptr<DataStream<E>> unexpected_stream;
    std::unique_ptr<DataStream<T>> expected_stream;
};

class SplitExpected {
   public:
    template <typename T, typename E>
    SplitExpectedResult<T, E> operator()(std::unique_ptr<DataStream<std::expected<T, E>>> stream) const {
        auto unexpected_stream = std::make_unique<VectorDataStream<E>>();
        auto expected_stream = std::make_unique<VectorDataStream<T>>();

        auto it = stream->begin();
        auto end = stream->end();

        while (!it.getIsEnd()) {
            const auto& value = *it;
            if (value.has_value()) {
                expected_stream->add(*value);
            } else {
                unexpected_stream->add(value.error());
            }
            ++it;
        }

        return {std::move(unexpected_stream), std::move(expected_stream)};
    }
};

template <typename K, typename V>
struct KV {
    using key_type = K;
    using value_type = V;
    K key;
    V value;

    bool operator==(const KV& other) const {
        return key == other.key && value == other.value;
    }
};

template <typename RightStream>
auto Join(std::unique_ptr<RightStream> right) {
    return [right = std::move(right)](auto&& left) mutable
               -> std::unique_ptr<DataStream<JoinResult<std::string, std::string>>> {
        auto left_strings = std::forward<decltype(left)>(left) | Transform([](const auto& kv) { return kv.value; });

        auto right_strings = std::move(right) | Transform([](const auto& kv) { return kv.value; });

        return std::make_unique<JoinedDataStream<std::string, std::string>>(
            std::move(left_strings),
            std::move(right_strings));
    };
}

template <typename R, typename LKey, typename RKey>
auto Join(std::unique_ptr<DataStream<R>> right, LKey lkey, RKey rkey) {
    return [right = std::move(right), lkey, rkey](auto&& left) mutable {
        using L = typename std::decay_t<decltype(*left)>::value_type;
        using Res = JoinResult<L, R>;

        std::unordered_multimap<decltype(rkey(std::declval<R>())), R> right_map;
        auto rit = right->begin();
        while (rit != right->end()) {
            auto val = *rit;
            right_map.emplace(rkey(val), val);
            ++rit;
        }

        auto result = std::make_unique<VectorDataStream<Res>>();
        auto lit = left->begin();
        while (lit != left->end()) {
            auto lval = *lit;
            auto key = lkey(lval);
            auto range = right_map.equal_range(key);

            if (range.first == range.second) {
                result->add(Res{lval, std::nullopt});
            } else {
                for (auto it = range.first; it != range.second; ++it) {
                    result->add(Res{lval, it->second});
                }
            }
            ++lit;
        }

        return result;
    };
}

template <typename T>
inline std::unique_ptr<DataStream<T>> AsDataFlow(const std::vector<T>& data) {
    return std::make_unique<VectorDataStream<T>>(data);
}

inline std::unique_ptr<DataStream<std::string>> AsDataFlow(const std::vector<std::stringstream>& streams) {
    auto combined_stream = std::make_unique<VectorDataStream<std::string>>();
    for (const auto& stream : streams) {
        combined_stream->add(stream.str());
    }
    return combined_stream;
}

template <typename T>
std::unique_ptr<DataStream<T>> AsDataFlow(std::vector<T>&& data) {
    return std::make_unique<VectorDataStream<T>>(std::move(data));
}

template <typename T, typename F>
auto operator|(std::unique_ptr<DataStream<T>> stream, F&& func) {
    return func(std::move(stream));
}

inline std::unique_ptr<DataStream<fs::path>> Dir(const std::string& path, bool recursive) {
    return std::make_unique<DirStream>(path, recursive);
}