package com.example.demo.service;

import com.example.demo.model.JobPosting;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class JobService {
    private List<JobPosting> jobPool = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    private double globalAvgMaxSalary = 0.0;
    private double globalAvgMinSalary = 0.0;

    @PostConstruct
    public void loadData(){
        try{
            System.out.println("Load data from JSON file");

            String path = System.getenv("DATA_FILE_PATH");
            if(path == null || path.isEmpty()){
                path = "/app/data.json";
            }

            File file = new File(path);
            jobPool = objectMapper.readValue(file, new TypeReference<List<JobPosting>>() {});
            System.out.println("Load done: " + jobPool.size() + " job(s)");

            calculateGlobalSalaryAverages();
        }catch (Exception e){
            System.out.println("Cannot read files, " + e.getMessage());
        }
    }

    public List<JobPosting> getJobBatch(int count){
        List<JobPosting> result = new ArrayList<>();
        int poolSize = jobPool.size();

        if(poolSize == 0) return result;

        long currentTime = System.currentTimeMillis();
        long futureTime = currentTime + (30L * 24 * 60 * 60 * 1000);

        for(int i = 0; i < count; i++){
            int index = ThreadLocalRandom.current().nextInt(poolSize);
            JobPosting job = jobPool.get(index);

            updateViews(job);
            updateApplies(job);

            job.setListedTime(String.valueOf(currentTime));
            job.setExpiry(String.valueOf(futureTime));

            fillMissingSalary(job);

            result.add(job);
        }
        return result;
    }

    private void fillMissingSalary(JobPosting job) {
        if(!isValidNumber(job.getMaxSalary())){
            double randomFactor = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
            double generatedSalary = globalAvgMaxSalary * randomFactor;
            job.setMaxSalary(String.valueOf((long) generatedSalary));
        }

        if (!isValidNumber(job.getMinSalary())) {
            double randomFactor = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
            double generatedSalary = globalAvgMinSalary * randomFactor;
            job.setMinSalary(String.valueOf((long) generatedSalary));
        }

        if (job.getPayPeriod() == null || job.getPayPeriod().isEmpty()) {
            job.setPayPeriod("YEARLY");
        }
    }

    private void calculateGlobalSalaryAverages(){
        double sumMax = 0;
        double sumMin = 0;
        int countMax = 0;
        int countMin = 0;

        for(JobPosting job : jobPool){
            if(isValidNumber(job.getMaxSalary())){
                sumMax += Double.parseDouble(job.getMaxSalary());
                countMax++;
            }

            if(isValidNumber(job.getMinSalary())){
                sumMin += Double.parseDouble(job.getMinSalary());
                countMin++;
            }
        }

        this.globalAvgMaxSalary = (countMax > 0) ? (sumMax / countMax) : 100000.0;
        this.globalAvgMinSalary = (countMin > 0) ? (sumMin / countMin) : 50000.0;

        System.out.println("Lương TB Min: " + globalAvgMinSalary + " | Lương TB Max: " + globalAvgMaxSalary);
    }

    private void updateViews(JobPosting job){
        try{
            double currentView = 0;
            if(job.getViews() != null && !job.getViews().isEmpty()){
                currentView = Double.parseDouble(job.getViews());
            }

            double increment = ThreadLocalRandom.current().nextInt(0, 6);
            currentView += increment;

            job.setViews(String.valueOf(currentView));
        }catch (Exception e){
            job.setViews("1.0");
        }
    }

    private void updateApplies(JobPosting job){
        try{
            double currentApplies = 0;
            if(job.getApplies() != null && !job.getApplies().isEmpty()){
                currentApplies = Double.parseDouble(job.getApplies());
            }

            double increment = ThreadLocalRandom.current().nextInt(0, 6);
            currentApplies += increment;

            job.setApplies(String.valueOf(currentApplies));
        }catch (Exception e){
            job.setApplies("0.0");
        }
    }

    private boolean isValidNumber(String numStr) {
        if (numStr == null || numStr.isEmpty()) return false;
        try {
            Double.parseDouble(numStr);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
