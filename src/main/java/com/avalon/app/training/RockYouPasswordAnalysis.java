package com.avalon.app.training;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RockYouPasswordAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("com.avalon.app.training.RockYouPasswordAnalysis").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("/user/raj_ops/RockYouPasswords/rockyou-withcount.txt").
                                        filter((String s) ->  s.trim().split(" ").length == 2);
        JavaPairRDD<String, Double> passwords = lines.mapToPair((String s) -> {
            String[] splits = s.trim().split(" ");
            return new Tuple2(splits[1], Double.parseDouble(splits[0]));
        });
        Double numTotalPasswords = passwords.values().reduce((Double x, Double y) -> x + y);
        JavaPairRDD<String, Double> passwordPercentages = passwords.mapValues((Double x) -> x / numTotalPasswords).cache();
        ArrayList<StatisticLogEntry> entries = new ArrayList<>();

        entries.add(ReportPercentageMatchingPattern(passwordPercentages, "[0-9]*", "have only numeric digits"));
        entries.add(ReportPercentageMatchingPattern(passwordPercentages, "[0-9]{4}", "are 4-digit PINs"));
        entries.add(ReportPercentageMatchingPattern(passwordPercentages, "[a-zA-Z]*", "have only alphabetical characters"));
        entries.add(ReportPercentageMatchingPattern(passwordPercentages, "(([a-zA-Z]+[0-9])|([0-9]+[a-zA-Z]))[0-9a-zA-Z]*", "contain both alphabetical and numeric characters"));
        entries.add(ReportPercentageMatchingPattern(passwordPercentages, "[a-zA-Z0-9]*", "have only alphabetical and/or numeric characters"));

        JavaPairRDD<Integer, Double> lengthDistribution = passwordPercentages.mapToPair((Tuple2<String, Double> x) -> new Tuple2<Integer, Double>(x._1().length(), x._2)).reduceByKey((Double x, Double y) -> x + y);
        List<Tuple2<Integer, Double>> lengthDistributionMap = lengthDistribution.sortByKey().collect();
        Double longPasswordsPercentage = 0.0;
        for (Tuple2<Integer, Double> entry: lengthDistributionMap) {
            if (entry._1() > 20)
            {
                longPasswordsPercentage += entry._2();
            }
            else
            {
                entries.add(new StatisticLogEntry("Percentage of Passwords which are " + entry._1().toString() + " characters long: ", entry._2()));
            }
        }
        entries.add(new StatisticLogEntry("Percentage of Passwords which are longer than 20 characters long: ", longPasswordsPercentage));

        JavaPairRDD<String, String> words = context.textFile("/user/raj_ops/RockYouPasswords/english.txt").mapToPair((String w) -> new Tuple2(w, ""));
        JavaPairRDD<String, Double> wordPasswords = passwordPercentages.join(words).mapValues((Tuple2<Double, String> x) -> x._1());
        Double wordPasswordPercentage = wordPasswords.values().reduce((Double x, Double y) -> x + y);
        entries.add(new StatisticLogEntry("Percentage of Passwords which are valid english words: ", wordPasswordPercentage));

        JavaPairRDD<String, String> firstNames = context.textFile("/user/raj_ops/RockYouPasswords/firstnames.txt").mapToPair((String w) -> new Tuple2(w, ""));
        JavaPairRDD<String, Double> firstNamePasswords = passwordPercentages.join(firstNames).mapValues((Tuple2<Double, String> x) -> x._1());
        Double firstNamePasswordPercentage = firstNamePasswords.values().reduce((Double x, Double y) -> x + y);
        entries.add(new StatisticLogEntry("Percentage of Passwords which match first names from Facebook: ", firstNamePasswordPercentage));

        Pattern pinPattern = Pattern.compile("[0-9]{4}");
        JavaPairRDD<String, Double> pinPasswords = passwords.filter((Tuple2<String, Double> x) -> pinPattern.matcher(x._1()).matches()).cache();
        Double numberOfPINPasswords = pinPasswords.values().reduce((Double x, Double y) -> x + y);
        JavaPairRDD<String, Double> pinPercentages = pinPasswords.mapValues((Double x) -> x / numberOfPINPasswords);
        entries.add(ReportPercentageOfPINsMatchingPattern(pinPercentages, "((19)|(20))[0-9]{2}", "appear to be years"));
        entries.add(ReportPercentageOfPINsMatchingPattern(pinPercentages, "([0-9])\\1\\1\\1", "contains the same digit repeated"));
        entries.add(ReportPercentageOfPINsMatchingPattern(pinPercentages, "([0-9])([0-9])\\1\\2", "contains the same two digits repeated"));

        System.out.println("---------- Password Statistics ----------");
        for (StatisticLogEntry entry : entries) {
            System.out.println(entry.toString());
        }
    }

    private static StatisticLogEntry ReportPercentageMatchingPattern(JavaPairRDD<String, Double> passwordPercentages, String pattern, String description)
    {
        Pattern matchingPattern = Pattern.compile(pattern);
        JavaPairRDD<String, Double> matchingPasswords = passwordPercentages.filter((Tuple2<String, Double> x) -> matchingPattern.matcher(x._1()).matches());
        Double matchingPercentage = matchingPasswords.values().reduce((Double x, Double y) -> x + y);
        return new StatisticLogEntry("Percentage of Passwords which " + description + ": ", matchingPercentage);
    }

    private static StatisticLogEntry ReportPercentageOfPINsMatchingPattern(JavaPairRDD<String, Double> passwordPercentages, String pattern, String description)
    {
        Pattern matchingPattern = Pattern.compile(pattern);
        JavaPairRDD<String, Double> matchingPasswords = passwordPercentages.filter((Tuple2<String, Double> x) -> matchingPattern.matcher(x._1()).matches());
        Double matchingPercentage = matchingPasswords.values().reduce((Double x, Double y) -> x + y);
        return new StatisticLogEntry("Percentage of PINs which " + description + ": ", matchingPercentage);
    }
}