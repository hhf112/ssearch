#include "../ssearch.hpp"
#include <gtest/gtest.h>

TEST(ssearch_test, searchText) { 
	ssearch::SE search;
	std::string sample = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since 1966, when designers at Letraset and James Mosley, the librarian at St Bride Printing Library in London, took a 1914 Cicero translation and scrambled it to make dummy text for Letraset's Body Type sheets. It has survived not only many decades, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised thanks to these sheets and more recently with desktop publishing software like Aldus PageMaker and Microsoft Word including versions of Lorem Ipsum.";

	int32_t cnt = 0;
	search.searchText(sample, "It", sample.begin(), sample.end(), [&cnt](ssearch::Pos &&pos) { ++cnt; });
	ASSERT_EQ(cnt, 2);
}


TEST(ssearch_test, threadedSearchText) { 
	ssearch::SE search;
	std::string sample = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since 1966, when designers at Letraset and James Mosley, the librarian at St Bride Printing Library in London, took a 1914 Cicero translation and scrambled it to make dummy text for Letraset's Body Type sheets. It has survived not only many decades, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised thanks to these sheets and more recently with desktop publishing software like Aldus PageMaker and Microsoft Word including versions of Lorem Ipsum.";

	int32_t cnt = 0;
	search.threadedSearchText(sample, "It", [&cnt](ssearch::Pos &&pos) { ++cnt; });
	ASSERT_EQ(cnt >= 2, true);
}

TEST(ssearch_test, searchFile) {
	ssearch::SE search;
	int32_t cnt = 0;
	search.searchFile("/home/hrsh/dev/ssearch/100mb.txt", "example", [&cnt](ssearch::Pos &&pos) { ++cnt; });
	ASSERT_EQ(cnt >= 2'688'657, true);
}


TEST(ssearch_test, threadedSearchFile) {
	ssearch::SE search;
	std::atomic<int32_t> cnt = 0;
	search.searchFile("/home/hrsh/dev/ssearch/100mb.txt", "example", [&cnt](ssearch::Pos &&pos) { ++cnt; });
	ASSERT_EQ(cnt >= 2'688'657, true);
}
