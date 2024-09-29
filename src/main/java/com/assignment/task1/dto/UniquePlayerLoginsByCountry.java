package com.assignment.task1.dto;

import lombok.Data;

@Data
public class UniquePlayerLoginsByCountry {
    private String date;
    private String hour;
    private String minute;
    private String metricName;
    private String country;
    private int loginCount;
}