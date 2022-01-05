package com.datapipeline;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.ParseException;

public class EpochTimeConverterTest {

    @Test
    public void testGetEpochTime() throws ParseException {
        // Given
        String input = "2021/12/28 0:00:00";
        // When
        long actual = EpochTimeConvertor.getEpochTime(input);


        // Then
        long expected =  1640649600L;
        Assert.assertEquals(actual,expected);
    }
}
