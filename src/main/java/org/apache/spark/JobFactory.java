package org.apache.spark;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

public class JobFactory {
    Job<String> createJob(String command) {
        return new Job<String>() {
            @Override
            public String call(JobContext jobContext) throws Exception {
                return null;
            }
        };
    }
}
