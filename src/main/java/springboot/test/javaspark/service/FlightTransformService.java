package springboot.test.javaspark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FlightTransformService {
    @Autowired
    private SparkSession sparkSession;

    public void flight() {
        Dataset<Row> flightDF = sparkSession.read().option("multiLine", true)
                .json("src/main/resources/search_flight.json");
        Dataset<Row> legDF = flightDF
                .withColumn("legDescs", functions.explode_outer(flightDF.col("groupedItineraryResponse.legDescs")))
                .drop("groupedItineraryResponse");

        legDF = legDF
                .withColumn("legId", legDF.col("legDescs.id"))
                .withColumn("schedule", functions.explode_outer(legDF.col("legDescs.schedules")));

        legDF = legDF
                .select("*")
                .withColumn("scheduleId", legDF.col("schedule.ref"))
                .drop("legDescs")
                .drop("schedule");

        legDF.show();

        Dataset<Row> scheduleDF = flightDF
                .withColumn("scheduleDescs", functions.explode_outer(flightDF.col("groupedItineraryResponse.scheduleDescs")))
                .drop("groupedItineraryResponse");

        scheduleDF = scheduleDF
                .withColumn("scheduleId", scheduleDF.col("scheduleDescs.id"))
                .withColumn("depAirPort", scheduleDF.col("scheduleDescs.departure.airport"))
                .withColumn("depCity", scheduleDF.col("scheduleDescs.departure.city"))
                .withColumn("depCountry", scheduleDF.col("scheduleDescs.departure.country"))
                .withColumn("depTime", scheduleDF.col("scheduleDescs.departure.time"))
                .withColumn("arrAirPort", scheduleDF.col("scheduleDescs.arrival.airport"))
                .withColumn("arrCity", scheduleDF.col("scheduleDescs.arrival.city"))
                .withColumn("arrCountry", scheduleDF.col("scheduleDescs.arrival.country"))
                .withColumn("arrTime", scheduleDF.col("scheduleDescs.arrival.time"))
                .drop("scheduleDescs");

        scheduleDF.show();

        Dataset<Row> itineraryDF = flightDF
                .withColumn("itineraryGroups", functions.explode_outer(flightDF.col("groupedItineraryResponse.itineraryGroups")))
                .drop("groupedItineraryResponse");


        itineraryDF = itineraryDF
                        .withColumn("groupDescription",itineraryDF.col("itineraryGroups.groupDescription"))
                        .withColumn("itineraries", functions.explode_outer(itineraryDF.col("itineraryGroups.itineraries")))
                        .drop("itineraryGroups");

        itineraryDF = itineraryDF
                .withColumn("legDescriptions", functions.explode_outer(itineraryDF.col("groupDescription.legDescriptions")))
                .withColumn("itineraries",itineraryDF.col("itineraries"))
                .drop("groupDescription");

        itineraryDF = itineraryDF
                .withColumn("departureLocation", itineraryDF.col("legDescriptions.departureLocation"))
                .withColumn("arrivalLocation", itineraryDF.col("legDescriptions.arrivalLocation"))
                .withColumn("departureDate", itineraryDF.col("legDescriptions.departureDate"))
                .withColumn("itineraryId", itineraryDF.col("itineraries.id"))
                .withColumn("leg", functions.explode_outer(itineraryDF.col("itineraries.legs")))
                .drop("legDescriptions")
                .drop("itineraries");

        itineraryDF = itineraryDF
                .select("*")
                .withColumn("legId", itineraryDF.col("leg.ref"))
                .drop("leg");

        itineraryDF = itineraryDF
                .select("*");

        itineraryDF.show();

        Dataset<Row> legScheduleDF = legDF.join(scheduleDF, legDF.col("scheduleId")
                .equalTo(scheduleDF.col("scheduleId")))
                .drop(legDF.col("scheduleId"));

        legScheduleDF.show();

        Dataset<Row> itineraryLegScheduleDF = itineraryDF.join(legScheduleDF, itineraryDF.col("legId").equalTo(
                legScheduleDF.col("legId")))
                .drop(itineraryDF.col("legId"));

        itineraryLegScheduleDF.show();
    }

    public void flightTransform() {
        Dataset<Row> flightDF = sparkSession.read().option("multiLine", true)
                .json("src/main/resources/flight.json");

        Dataset<Row> airlinesDF= flightDF
                .withColumn("itineraryGroups", functions.explode_outer(flightDF.col("groupedItineraryResponse.itineraryGroups")))
                .drop("groupedItineraryResponse");

        airlinesDF = airlinesDF
                .withColumn("itineraries", functions.explode_outer(airlinesDF.col("itineraryGroups.itineraries")))
                .drop("itineraryGroups");

        airlinesDF = airlinesDF
                .withColumn("pricingInformation", functions.explode_outer(airlinesDF.col("itineraries.pricingInformation")))
                .drop("itineraries");

        airlinesDF = airlinesDF
                .withColumn("passengerInfoList", functions.explode_outer(airlinesDF.col("pricingInformation.fare.passengerInfoList")))
                .drop("pricingInformation");

        airlinesDF = airlinesDF
                .withColumn("baggageInformation", functions.explode(airlinesDF.col("passengerInfoList.passengerInfo.baggageInformation")));

        airlinesDF = airlinesDF
                .withColumn("airlineCode", airlinesDF.col("baggageInformation.airlineCode"));
        airlinesDF.show();
    }

    public void transform() {
        Dataset<Row> flightDF = sparkSession.read().option("multiLine", true)
                .json("src/main/resources/flight.json");

        flightDF = flightDF
                .select("*")
                .withColumn("bad",
                        functions.explode(flightDF.col("groupedItineraryResponse.baggageAllowanceDescs")))
                .withColumn("bcd",
                        functions.explode_outer(flightDF.col("groupedItineraryResponse.baggageChargeDescs")))
                .withColumn("bfd",
                        functions.explode_outer(flightDF.col("groupedItineraryResponse.brandFeatureDescs")))
                .withColumn("csd",
                        functions.explode_outer(flightDF.col("groupedItineraryResponse.cacheSourceDescs")))
                .withColumn("fcd",
                        functions.explode_outer(flightDF.col("groupedItineraryResponse.fareComponentDescs")));


        flightDF = flightDF
                .select("bad", "bcd", "bfd",
                        "csd", "fcd")
                .withColumn("bad_id", flightDF.col("bad.id"))
                .withColumn("bad_piece_count", flightDF.col("bad.pieceCount"))
                .withColumn("bad_unit", flightDF.col("bad.unit"))
                .withColumn("bad_weight", flightDF.col("bad.weight"))
                .withColumn("fcd_id", flightDF.col("fcd.id"))
                .drop("bad");
        flightDF.show();
    }
}
