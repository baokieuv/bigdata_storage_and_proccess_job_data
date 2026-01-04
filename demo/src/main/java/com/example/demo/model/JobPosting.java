package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
@Data
public class JobPosting {

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("company_name")
    private String companyName;

    private String title;
    private String description;

    @JsonProperty("max_salary")
    private String maxSalary;

    @JsonProperty("pay_period")
    private String payPeriod;

    private String location;

    @JsonProperty("company_id")
    private String companyId;

    private String views;

    @JsonProperty("med_salary")
    private String medSalary;

    @JsonProperty("min_salary")
    private String minSalary;

    @JsonProperty("formatted_work_type")
    private String formattedWorkType;

    private String applies;

    @JsonProperty("original_listed_time")
    private String originalListedTime;

    @JsonProperty("remote_allowed")
    private String remoteAllowed;

    @JsonProperty("job_posting_url")
    private String jobPostingUrl;

    @JsonProperty("application_url")
    private String applicationUrl;

    @JsonProperty("application_type")
    private String applicationType;

    private String expiry;

    @JsonProperty("closed_time")
    private String closedTime;

    @JsonProperty("formatted_experience_level")
    private String formattedExperienceLevel;

    @JsonProperty("skills_desc")
    private String skillsDesc;

    @JsonProperty("listed_time")
    private String listedTime;

    @JsonProperty("posting_domain")
    private String postingDomain;

    private String sponsored;

    @JsonProperty("work_type")
    private String workType;

    private String currency;

    @JsonProperty("compensation_type")
    private String compensationType;

    @JsonProperty("normalized_salary")
    private String normalizedSalary;

    @JsonProperty("zip_code")
    private String zipCode;

    private String fips;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMaxSalary() {
        return maxSalary;
    }

    public void setMaxSalary(String maxSalary) {
        this.maxSalary = maxSalary;
    }

    public String getPayPeriod() {
        return payPeriod;
    }

    public void setPayPeriod(String payPeriod) {
        this.payPeriod = payPeriod;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getCompanyId() {
        return companyId;
    }

    public void setCompanyId(String companyId) {
        this.companyId = companyId;
    }

    public String getViews() {
        return views;
    }

    public void setViews(String views) {
        this.views = views;
    }

    public String getMedSalary() {
        return medSalary;
    }

    public void setMedSalary(String medSalary) {
        this.medSalary = medSalary;
    }

    public String getMinSalary() {
        return minSalary;
    }

    public void setMinSalary(String minSalary) {
        this.minSalary = minSalary;
    }

    public String getFormattedWorkType() {
        return formattedWorkType;
    }

    public void setFormattedWorkType(String formattedWorkType) {
        this.formattedWorkType = formattedWorkType;
    }

    public String getApplies() {
        return applies;
    }

    public void setApplies(String applies) {
        this.applies = applies;
    }

    public String getOriginalListedTime() {
        return originalListedTime;
    }

    public void setOriginalListedTime(String originalListedTime) {
        this.originalListedTime = originalListedTime;
    }

    public String getRemoteAllowed() {
        return remoteAllowed;
    }

    public void setRemoteAllowed(String remoteAllowed) {
        this.remoteAllowed = remoteAllowed;
    }

    public String getJobPostingUrl() {
        return jobPostingUrl;
    }

    public void setJobPostingUrl(String jobPostingUrl) {
        this.jobPostingUrl = jobPostingUrl;
    }

    public String getApplicationUrl() {
        return applicationUrl;
    }

    public void setApplicationUrl(String applicationUrl) {
        this.applicationUrl = applicationUrl;
    }

    public String getApplicationType() {
        return applicationType;
    }

    public void setApplicationType(String applicationType) {
        this.applicationType = applicationType;
    }

    public String getExpiry() {
        return expiry;
    }

    public void setExpiry(String expiry) {
        this.expiry = expiry;
    }

    public String getClosedTime() {
        return closedTime;
    }

    public void setClosedTime(String closedTime) {
        this.closedTime = closedTime;
    }

    public String getFormattedExperienceLevel() {
        return formattedExperienceLevel;
    }

    public void setFormattedExperienceLevel(String formattedExperienceLevel) {
        this.formattedExperienceLevel = formattedExperienceLevel;
    }

    public String getSkillsDesc() {
        return skillsDesc;
    }

    public void setSkillsDesc(String skillsDesc) {
        this.skillsDesc = skillsDesc;
    }

    public String getListedTime() {
        return listedTime;
    }

    public void setListedTime(String listedTime) {
        this.listedTime = listedTime;
    }

    public String getPostingDomain() {
        return postingDomain;
    }

    public void setPostingDomain(String postingDomain) {
        this.postingDomain = postingDomain;
    }

    public String getSponsored() {
        return sponsored;
    }

    public void setSponsored(String sponsored) {
        this.sponsored = sponsored;
    }

    public String getWorkType() {
        return workType;
    }

    public void setWorkType(String workType) {
        this.workType = workType;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getCompensationType() {
        return compensationType;
    }

    public void setCompensationType(String compensationType) {
        this.compensationType = compensationType;
    }

    public String getNormalizedSalary() {
        return normalizedSalary;
    }

    public void setNormalizedSalary(String normalizedSalary) {
        this.normalizedSalary = normalizedSalary;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getFips() {
        return fips;
    }

    public void setFips(String fips) {
        this.fips = fips;
    }
}