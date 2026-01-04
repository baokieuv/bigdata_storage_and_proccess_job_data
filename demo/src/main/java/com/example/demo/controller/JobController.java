package com.example.demo.controller;

import com.example.demo.model.JobPosting;
import com.example.demo.service.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class JobController {

    @Autowired
    private JobService jobService;

    @GetMapping("/api/jobs")
    public List<JobPosting> getJobs(@RequestParam(defaultValue = "10") int limit) {
        return jobService.getJobBatch(limit);
    }
}