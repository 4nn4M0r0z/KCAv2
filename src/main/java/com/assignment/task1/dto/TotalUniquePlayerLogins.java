package com.assignment.task1.dto;

import lombok.Data;

@Data
public class TotalUniquePlayerLogins {
    private String date;
    private String hour;
    private String minute;
    private String metricName;
    private int loginCount;
}