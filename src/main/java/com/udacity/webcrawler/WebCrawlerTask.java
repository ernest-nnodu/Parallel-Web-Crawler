package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

public class WebCrawlerTask extends RecursiveTask<CrawlResult> {

    private final Clock clock;
    private final PageParserFactory parserFactory;
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;
    private final ConcurrentMap<String, Integer> counts;
    private final Set<String> visitedUrls;

    @Inject
    public WebCrawlerTask(Clock clock, PageParserFactory parserFactory, String url,
                          Instant deadline,
                          int maxDepth, List<Pattern> ignoredUrls,
                          ConcurrentMap<String, Integer> counts,
                          Set<String> visitedUrls) {
        this.clock = clock;
        this.parserFactory = parserFactory;
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.ignoredUrls = ignoredUrls;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
    }

    @Override
    protected CrawlResult compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return new CrawlResult.Builder()
                    .setWordCounts(counts)
                    .setUrlsVisited(visitedUrls.size())
                    .build();
        }

        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return new CrawlResult.Builder()
                        .setWordCounts(counts)
                        .setUrlsVisited(visitedUrls.size())
                        .build();
            }
        }

        if (visitedUrls.contains(url)) {
            return new CrawlResult.Builder()
                    .setWordCounts(counts)
                    .setUrlsVisited(visitedUrls.size())
                    .build();
        }

        visitedUrls.add(url);
        PageParser.Result result = parserFactory.get(url).parse();

        result.getWordCounts().forEach((word, count) ->
                counts.merge(word, count, Integer::sum));

        List<WebCrawlerTask> subtasks = result.getLinks().stream()
                .map(link -> new Builder()
                        .setClock(clock)
                        .setParserFactory(parserFactory)
                        .setUrl(link)
                        .setDeadline(deadline)
                        .setMaxDepth(maxDepth - 1)
                        .setIgnoredUrls(ignoredUrls)
                        .setCounts(counts)
                        .setVisitedUrls(visitedUrls)
                        .build())
                .toList();
        invokeAll(subtasks);

        return new CrawlResult.Builder()
                .setWordCounts(counts)
                .setUrlsVisited(visitedUrls.size())
                .build();
    }

    public static class Builder {

        private Clock clock;
        private PageParserFactory parserFactory;
        private String url;
        private Instant deadline;
        private int maxDepth;
        private List<Pattern> ignoredUrls;
        private ConcurrentMap<String, Integer> counts;
        private Set<String> visitedUrls;

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setParserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public Builder setCounts(ConcurrentMap<String, Integer> counts) {
            this.counts = counts;
            return this;
        }

        public Builder setVisitedUrls(Set<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public WebCrawlerTask build() {
            return new WebCrawlerTask(clock,
                    parserFactory,
                    url,
                    deadline,
                    maxDepth,
                    ignoredUrls,
                    counts, visitedUrls);
        }
    }
}
