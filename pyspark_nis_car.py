import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as sparkFun
tableSize = 30

def getRegionByCoordinates(param_x, param_y, size, df):
    sizeNum = 0.005
    if size == "SMALL":
        sizeNum = 0.005
    elif size == "MEDIUM":
        sizeNum = 0.01
    elif size == "LARGE":
        sizeNum = 0.02
    elif size == "ENORMOUS":
        sizeNum = 0.05
    tmp = df.filter((df.vehicle_x > param_x - sizeNum) &
                    (df.vehicle_x < param_x + sizeNum) &
                    (df.vehicle_y > param_y - sizeNum) &
                    (df.vehicle_y < param_y + sizeNum))

    return tmp


def getRegionByTimestep(timestepStart, timestepSize, df):
    tmp = df.filter((df.timestep_time > timestepStart) &
                    (df.timestep_time < timestepStart + timestepSize))
    return tmp


def groupByLane(df):
    df_lane = df.groupBy('vehicle_lane')
    speed = df_lane.agg(sparkFun.min('vehicle_speed').alias("Min speed"),
                        sparkFun.max('vehicle_speed').alias("Max speed"),
                        sparkFun.avg('vehicle_speed').alias("Average speed"),
                        sparkFun.stddev('vehicle_speed').alias("Std speed"))

    speed.show(tableSize)

    pos = df_lane.agg(sparkFun.max('vehicle_pos').alias("Street length"),
                      sparkFun.avg('vehicle_pos').alias("Avg position"),
                      sparkFun.stddev('vehicle_pos').alias("STD pos"))

    pos.show(tableSize)


def showStatistics(df):
    speed = df.agg(sparkFun.max('vehicle_speed').alias("Max speed"),
                   sparkFun.min('vehicle_speed').alias("Min speed"),
                   sparkFun.avg('vehicle_speed').alias("Average speed"),
                   sparkFun.stddev('vehicle_speed').alias("Std speed"))

    speed.withColumn("Count", sparkFun.lit(df.count())).show()

    angle = df.agg(sparkFun.max('vehicle_angle').alias("Max angle"),
                   sparkFun.min('vehicle_angle').alias("Min angle"),
                   sparkFun.avg('vehicle_angle').alias("Average angle"),
                   sparkFun.stddev('vehicle_angle').alias("Std angle"))

    angle.withColumn("Count", sparkFun.lit(df.count())).show()

    pos = df.agg(sparkFun.max('vehicle_pos').alias("Max pos"),
                 sparkFun.min('vehicle_pos').alias("Min pos"),
                 sparkFun.avg('vehicle_pos').alias("Average pos"),
                 sparkFun.stddev('vehicle_pos').alias("Std pos"))

    pos.withColumn("Count", sparkFun.lit(df.count())).show()

# traka u kojoj se prosecno najduze ceka
# sat u kome se prosecno najduze ceka
def theLongestWaitingTime(df, value):

    #za value False, trazimo najzaguseniju traku
    #za value True, trazimo najzaguseniji sat
    if(value):
        df_group = df.groupBy('hour')
    else:
        df_group = df.groupBy('vehicle_lane')

    df_means = df_group.agg(sparkFun.avg("vehicle_waiting").alias("mean_vehicle_waiting"))
    df_means_sorted = df_means.sort("mean_vehicle_waiting", ascending=False)
    max_group = df_means_sorted.first()
    max_mean = max_group.mean_vehicle_waiting

    if(value):
        max_hour = max_group.hour
        print("\n*\n*\n*\n*\n")
        print("The busiest hour: " + str(max_hour) + ", max mean waiting time: " + str(max_mean))
        print("\n*\n*\n*\n*\n")
    else:
        max_vehicle_lane = max_group.vehicle_lane
        print("\n*\n*\n*\n*\n")
        print("Max_vehicle_lane: " + str(max_vehicle_lane) + ", max mean waiting time: " + str(max_mean))
        print("\n*\n*\n*\n*\n")

# funkcija koja odredjuje gde ima najvise vozila u svakom satu
def busiest_lane(df):

    # grupisanje po satu i po traci
    df_grouped = df.groupBy(df["hour"], df["vehicle_lane"])

    # u svakom satu odrediti broj elemenata u svakoj traci
    df_counts = df_grouped.agg(sparkFun.count("*").alias("count"))

    #odrediti traku koja ima najveci broj elemenata
    max_counts = df_counts.groupBy("hour").agg(sparkFun.max("count").alias("max_count"))
    df_counts = df_counts.withColumnRenamed("hour", "hour_count")

    # za svaki sat odrediti traku kroz koju prodje najvise elemenata
    result = max_counts.join(df_counts, (max_counts["hour"] == df_counts["hour_count"]) & (
                max_counts["max_count"] == df_counts["count"]))
    result = result.select("hour", "vehicle_lane", "max_count")
    result.show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Demo").master("local[*]").getOrCreate()
    dataset = spark.read.option("inferSchema", True).option("header", True).csv(sys.argv[1])

    mode = int(sys.argv[2])

    if mode == 2:
        groupByLane(dataset)
    elif mode == 1:

        x_coord = float(sys.argv[3])
        y_coord = float(sys.argv[4])
        size = sys.argv[5]

        df = getRegionByCoordinates(x_coord, y_coord, size, dataset)

        numOfParameters = len(sys.argv)
        if numOfParameters == 8:
            timestepStart = float(sys.argv[6])
            timestepSize = float(sys.argv[7])
            df = getRegionByTimestep(timestepStart, timestepSize, df)

        showStatistics(df)
    elif mode == 3:
        theLongestWaitingTime(df=dataset, value=False)
        theLongestWaitingTime(df=dataset, value=True)
        busiest_lane(dataset)