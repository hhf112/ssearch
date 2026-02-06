#include <map>
#include <string>
#include <vector>

struct PatternCacheObj {
  std::vector<size_t> shift, bpos;
  std::vector<ptrdiff_t> badchars;
  int NCHARS;

  PatternCacheObj() = default;
  PatternCacheObj(int nchars, size_t patternlen) : NCHARS{nchars} {
    shift.resize(patternlen + 1);
    bpos.resize(patternlen + 1);
    badchars.resize(nchars, -1);
  }
};

class PatternCacheResolvr {
 public:
  PatternCacheResolvr() = default;

  /*@brief fetch from or create in preprocessed pattern PatternCacheObj*/
  inline const PatternCacheObj &queryPatternCache(int nchars, const std::string &str) {
    if (page_.count(str)) return page_[str];

    int n = str.length();
    PatternCacheObj data(nchars, str.length());
    badchh(data.badchars, str, n);
    ssuffix(data.shift, data.bpos, str, n);
    case1(data.shift, data.bpos, str, n);

    auto [newData, _] = page_.emplace(str, std::move(data));
    return newData->second;
  }

 private:
  /*@src
   * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-for-pattern-searchTexting*/
  inline void badchh(std::vector<ptrdiff_t> &badhcars, const std::string &str,
                     size_t size) {
    size_t i;
    for (i = 0; i < size; i++) badhcars[(int)str[i]] = i;
  }

  /*@src
   * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-good-suffix-heuristic*/
  inline void ssuffix(std::vector<size_t> &shift, std::vector<size_t> &bpos,
                      const std::string &pat, size_t m) {
    size_t i, j;
    j = bpos[0];
    for (i = 0; i <= m; i++) {
      if (shift[i] == m) shift[i] = j;
      if (i == j) j = bpos[j];
    }
  }

  /*@src
   * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-good-suffix-heuristic*/
  inline void case1(std::vector<size_t> &shift, std::vector<size_t> &bpos,
                    const std::string &pat, size_t m) {
    ptrdiff_t i = static_cast<ptrdiff_t>(m), j = static_cast<ptrdiff_t>(m + 1);
    bpos[i] = j;
    while (i > 0) {
      while (j <= m && pat[i - 1] != pat[j - 1]) {
        if (shift[j] == m) shift[j] = j - i;
        j = bpos[j];
      }
      i--;
      j--;
      bpos[i] = j;
    }
  }

  std::map<std::string, PatternCacheObj> page_;
};
