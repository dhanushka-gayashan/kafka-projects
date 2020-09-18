package com.dhanushka.kafka.util;

import com.dhanushka.kafka.GitHubSourceConnectorConfig;
import com.dhanushka.kafka.model.Issue;
import com.dhanushka.kafka.model.PullRequest;
import com.dhanushka.kafka.model.User;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.dhanushka.kafka.GitHubSchemas.*;

public class GitHubSourceTaskUtils {

    public static Map<String, String> sourcePartition(GitHubSourceConnectorConfig config) {
        Map<String, String> map = new HashMap<>();
        map.put(OWNER_FIELD, config.getOwnerConfig());
        map.put(REPOSITORY_FIELD, config.getRepoConfig());
        return map;
    }

    public static Map<String, String> sourceOffset(Instant updatedAt, Instant nextQuerySince, Integer nextPageToVisit) {
        Map<String, String> map = new HashMap<>();
        map.put(UPDATED_AT_FIELD, DateUtils.MaxInstant(updatedAt, nextQuerySince).toString());
        map.put(NEXT_PAGE_FIELD, nextPageToVisit.toString());
        return map;
    }

    public static Struct buildRecordKey(Issue issue, GitHubSourceConnectorConfig config){
        // Key Schema
        Struct key = new Struct(KEY_SCHEMA)
                .put(OWNER_FIELD, config.getOwnerConfig())
                .put(REPOSITORY_FIELD, config.getRepoConfig())
                .put(NUMBER_FIELD, issue.getNumber());

        return key;
    }

    public static Struct buildRecordValue(Issue issue){
        // Top Level Fields
        Struct valueStruct = new Struct(VALUE_SCHEMA)
                .put(URL_FIELD, issue.getUrl())
                .put(TITLE_FIELD, issue.getTitle())
                .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
                .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
                .put(NUMBER_FIELD, issue.getNumber())
                .put(STATE_FIELD, issue.getState());

        // User Is Mandatory Field
        User user = issue.getUser();
        Struct userStruct = new Struct(USER_SCHEMA)
                .put(USER_URL_FIELD, user.getUrl())
                .put(USER_ID_FIELD, user.getId())
                .put(USER_LOGIN_FIELD, user.getLogin());
        valueStruct.put(USER_FIELD, userStruct);

        // Pull Request Is Optional Field
        PullRequest pullRequest = issue.getPullRequest();
        if (pullRequest != null) {
            Struct prStruct = new Struct(PR_SCHEMA)
                    .put(PR_URL_FIELD, pullRequest.getUrl())
                    .put(PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
            valueStruct.put(PR_FIELD, prStruct);
        }

        return valueStruct;
    }

    public static SourceRecord generateSourceRecord(Issue issue, GitHubSourceConnectorConfig config, Instant nextQuerySince, Integer nextPageToVisit) {
        return new SourceRecord(
                sourcePartition(config),
                sourceOffset(issue.getUpdatedAt(), nextQuerySince, nextPageToVisit),
                config.getTopic(),
                null, // partition will be inferred by the framework
                KEY_SCHEMA,
                buildRecordKey(issue, config),
                VALUE_SCHEMA,
                buildRecordValue(issue),
                issue.getUpdatedAt().toEpochMilli());
    }
}
