package com.dhanushka.kafka;

import com.dhanushka.kafka.model.Issue;
import com.dhanushka.kafka.util.GitHubSourceTaskUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dhanushka.kafka.GitHubSchemas.*;

public class GitHubSourceTask extends SourceTask {

  static final Logger log = LoggerFactory.getLogger(GitHubSourceTask.class);

  public GitHubSourceConnectorConfig config;
  GitHubAPIHttpClient gitHubAPIHttpClient;

  protected Instant nextQuerySince;
  protected Integer lastIssueNumber;
  protected Integer nextPageToVisit = 1;
  protected Instant lastUpdatedAt;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> map) {
    //Do things here that are required to start your task. This could be open a connection to a database, etc.
    config = new GitHubSourceConnectorConfig(map);
    initializeLastVariables();
    gitHubAPIHttpClient = new GitHubAPIHttpClient(config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    gitHubAPIHttpClient.sleepIfNeed();

    // fetch data
    final ArrayList<SourceRecord> records = new ArrayList<>();
    JSONArray issues = gitHubAPIHttpClient.getNextIssues(nextPageToVisit, nextQuerySince);

    int issuesCount = issues.length();
    if (issuesCount > 0) log.info(String.format("Fetched %s record(s)", issuesCount));

    issues.forEach(object -> {
      Issue issue = Issue.fromJson((JSONObject) object);
      SourceRecord sourceRecord = GitHubSourceTaskUtils.generateSourceRecord(issue, config, nextQuerySince, nextPageToVisit);
      records.add(sourceRecord);
      lastUpdatedAt = issue.getUpdatedAt();
    });

    // if we have reached a full batch, we need to get the next one, otherwise set nextPageToVisit as 1st page
    if (issuesCount == 100){
      nextPageToVisit++;
    }
    else {
      nextQuerySince = lastUpdatedAt.plusSeconds(1);
      nextPageToVisit = 1;
      gitHubAPIHttpClient.sleep();
    }

    return records;
  }

  @Override
  public void stop() {
    //Do whatever is required to stop your task.
  }

  private void initializeLastVariables(){
    Map<String, Object> lastSourceOffset = context.offsetStorageReader().offset(GitHubSourceTaskUtils.sourcePartition(config));

    if( lastSourceOffset == null){
      // we haven't fetched anything yet, so we initialize to 7 days ago
      nextQuerySince = config.getSince();
      lastIssueNumber = -1;
    } else {

      Object updatedAt = lastSourceOffset.get(UPDATED_AT_FIELD);
      Object issueNumber = lastSourceOffset.get(NUMBER_FIELD);
      Object nextPage = lastSourceOffset.get(NEXT_PAGE_FIELD);

      if(updatedAt != null && (updatedAt instanceof String)){
        nextQuerySince = Instant.parse((String) updatedAt);
      }

      if(issueNumber != null && (issueNumber instanceof String)){
        lastIssueNumber = Integer.valueOf((String) issueNumber);
      }

      if (nextPage != null && (nextPage instanceof String)){
        nextPageToVisit = Integer.valueOf((String) nextPage);
      }
    }
  }
}