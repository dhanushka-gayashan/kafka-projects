package com.dhanushka.kafka.model;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

import static com.dhanushka.kafka.GitHubSchemas.*;

public class PullRequest {

    private String url;
    private String htmlUrl;
    private String diffUrl;
    private String patchUrl;
    private Map<String, Object> additionalProperties = new HashMap<>();

    public PullRequest() {
    }

    /**
     *
     * @param url
     * @param htmlUrl
     * @param diffUrl
     * @param patchUrl
     * @param additionalProperties
     */
    public PullRequest(String url, String htmlUrl, String diffUrl, String patchUrl, Map<String, Object> additionalProperties) {
        this.url = url;
        this.htmlUrl = htmlUrl;
        this.diffUrl = diffUrl;
        this.patchUrl = patchUrl;
        this.additionalProperties = additionalProperties;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public PullRequest withUrl(String url) {
        this.url = url;
        return this;
    }

    public String getHtmlUrl() {
        return htmlUrl;
    }

    public void setHtmlUrl(String htmlUrl) {
        this.htmlUrl = htmlUrl;
    }

    public PullRequest withHtmlUrl(String htmlUrl) {
        this.htmlUrl = htmlUrl;
        return this;
    }

    public String getDiffUrl() {
        return diffUrl;
    }

    public void setDiffUrl(String diffUrl) {
        this.diffUrl = diffUrl;
    }

    public PullRequest withDiffUrl(String diffUrl) {
        this.diffUrl = diffUrl;
        return this;
    }

    public String getPatchUrl() {
        return patchUrl;
    }

    public void setPatchUrl(String patchUrl) {
        this.patchUrl = patchUrl;
    }

    public PullRequest withPatchUrl(String patchUrl) {
        this.patchUrl = patchUrl;
        return this;
    }

    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public PullRequest withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    public static PullRequest fromJson(JSONObject pullRequest) {
        return new PullRequest()
                .withUrl(pullRequest.getString(PR_URL_FIELD))
                .withHtmlUrl(pullRequest.getString(PR_HTML_URL_FIELD));

    }
}
