package com.avalon.app.training;

/**
 * Created by davidmcginnis on 1/30/17.
 */
public class StatisticLogEntry {
    private Double _value;
    private String _description;

    public StatisticLogEntry(String description, Double value)
    {
        _description = description;
        _value = value;
    }

    @Override
    public String toString() {
        return _description + ": " + _value.toString();
    }
}
