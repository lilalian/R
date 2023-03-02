#####----- Install packages-----------------------------
install.packages("sparklyr")
library(sparklyr)

# support versions spark1.6/2.0/2.1/2.2/2.3/2.4/3.0/3.1/3.2/3.3
spark_available_versions(show_hadoop = F,    # Show Hadoop distributions
                         show_minor = F,     # Show minor Spark versions
                         show_future = F)    # show future versions haven't been released

#####----- Download and install Spark-------------------
spark_install()      # Installing Spark 2.4.8 for Hadoop 2.7 or later.
# Connection configuration
spark_config()
# Error in download.file(...) : 
# download from 'https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz' failed
# Cause:the download processing failed because of a timeout
# Set the timeout duration to more than the default value of 60 seconds

# set global options
options(timeout = 300)

# options(download.file.method = 'auto')

spark_install(version = "2.4", hadoop_version = "2.7")
# spark_uninstall(version = "2.4", hadoop_version = "2.7")

# utils::sessionInfo()

#####----- Connect to Spark clusters----------------------
# Spark cluster url to connect to
# Use "local" to connect to a local instance of Spark installed via spark install
sc<-spark_connect(master="local")
# spark_connect(master = "yarn-client")

# Before connection you need to have java installed(Java SE Development Kit 8u211)
# system("java -version")

# To visit spark web interface: /click spark in Connections
spark_web(sc)

# shiny::runGadget(sparklyr::connection_spark_shinyapp(),viewer=.rs.embeddedViewer)

#####----- Transfer data----------------------------------
# From our R environment to Spark
library(dplyr)
library(dbplyr)
library(nycflights13)
# The variable sc_* contains the info that points to the location 
# where the Spark session loaded the data to
sc_iris<-
  sc %>% 
  dplyr::copy_to(iris)
sc_cars<-
  sc %>% 
  sparklyr::sdf_copy_to(cars)
sc_flights<-copy_to(sc,flights)
sc_starwars<-copy_to(sc,starwars)
# Show datasets
dplyr::src_tbls(sc)
# Load csv data to Spark
sc_worker<-spark_read_csv(sc,
                          overwrite = T,
                          path = "C://Users//nan//Desktop//worker.csv")
sc_worker
# Write a spark dataframe to a csv
library(DBI)
spark_write_csv(sc_cars,
                #cars,
                path = "C://Users//nan//Documents//R Files//cars.csv")

#####-----Window functions------------------------------
dp1<-
  sc_starwars %>%   # use sc_starwars instead of starwars
  na.omit() %>%   
  dplyr::select(name,height,birth_year:gender,contains('color')) %>%
  filter(height>=mean(height,na.rm = T)) %>% 
  dplyr::mutate(rank=rank(birth_year)) %>%
  group_by(sex)   # if you want to rank in groups,use group_by before ranking.
# SQL
dbplyr::sql_render(dp1)
dplyr::show_query(dp1)

dp1

dp2<-
  sc_starwars %>%
  na.omit() %>%   
  dplyr::select(name,height,birth_year:gender,contains('color')) %>%
  #filter(height>=mean(height,na.rm = T)) %>% 
  group_by(sex) %>% 
  filter(height>=mean(height,na.rm = T)) %>%
  dplyr::mutate(rank=rank(-height))
dp2  

# Copy data from Spark into R’s memory
c1<-collect(dp1)
c1
c2<-collect(dp2)
c2
# Use SQL Query
library(DBI)
cars_5<- DBI::dbGetQuery(sc, "SELECT * FROM cars order by speed desc,dist desc LIMIT 5")
cars_5
# Translate
# group_max_min_avg<-
#   sc_iris %>%
#   group_by(Species) %>%
#   summarise(max_spl=max(Sepal_Length,na.rm = T),
#             mean_spl=mean(Sepal_Length,na.rm = T),
#             min_spl=min(Sepal_Length,na.rm = T)
#             )
# dbplyr::sql_render(group_max_min_avg)
# <SQL> SELECT
# `Species`,
# MAX(`Sepal_Length`) AS `max_spl`,
# AVG(`Sepal_Length`) AS `mean_spl`,
# MIN(`Sepal_Length`) AS `min_spl`
# FROM `iris`
# GROUP BY `Species`


#####-----Join-----------------------------------
library(DBI)
library(RMariaDB)
con1<-dbConnect(RMariaDB::MariaDB(),
                dbname = 'spr',
                username = 'root',
                password = rstudioapi::askForPassword("Enter your Password:"),
                host = 'localhost'
                )
dbListTables(con1)
department<-dbReadTable(con1,'department')
staff<-dbReadTable(con1,'staff')
dbDisconnect(con1)

department
staff
# Copy to spark
sc_dp<-copy_to(sc,department)
sc_staff<-copy_to(sc,staff)

# Inner_join
join_sta_dp<-
  dplyr::inner_join(sc_staff,sc_dp,
                    by=c('dp_id'='id'),
                    copy = FALSE,  # allows you to join tables across srcs
                    suffix = c("_sc_staff", "_sc_dp"), # disambiguate non-joined duplicate variables
                    na_matches = c("na","never")
                    # "na" treats two NA or two NaN values as equal
                    # "never" treats two NA or two NaN values as different
                    #  and will never match them together or to any other values
                  )
# To combine its arguments to form a vector
c('a','b','c')
join_sta_dp
# --
dplyr::inner_join(sc_staff,sc_dp)
dplyr::inner_join(sc_staff,sc_dp,by='id')
# Outer join
dplyr::left_join(sc_staff,sc_dp,
                 by=c('dp_id'='id')
                 )
dplyr::left_join(sc_dp,sc_staff,
                  by=c('id'='dp_id'), 
                  suffix = c("_sc_dp", "_sc_staff")
                 )
#####-----Sample---------------------------------

# You can use sample_n() and sample_frac() to take a random sample of rows
# use sample_n() for a fixed number and sample_frac() for a fixed fraction.
sample_n(sc_cars, 10)

sample_frac(sc_cars, 0.1)  # 50*0.1=5

#####----- #Hive functions-------------------------
# Many of Hive’s built-in functions (UDF) and built-in aggregate functions (UDAF) 
# can be called inside dplyr’s mutate and summarize.
dp3<-
  sc_flights %>% 
  mutate(
    flight_date = paste(year,month,day,sep="-"),
    days_since = datediff(current_date(), flight_date)
    ) %>%
  group_by(flight_date,days_since) %>%
  count() %>%
  arrange(-days_since)
c3<-collect(dp3)
dbplyr::sql_render(dp3)

#####-----MLlib in Sparklyr----------------------
# Divide the dataset into training and testing sets
# 把数据集分为训练集(70%数据用于训练模型)和测试集
partitions <- 
  sc_cars %>%
  sdf_random_split(training = 0.7, test = 0.3,seed = 1)
partitions
# Fitting of a model
linear_fit<-
  sc_cars %>%    # partitions$traing %>% 
  sparklyr::ml_linear_regression(dist~speed)
linear_fit   # dist=3.933194*speed-16.643667
summary(linear_fit)
# Extract the slope and the intercept into discrete R variables
# 将斜率和截距提取到离散 R 变量中,用于绘制直线
spark_slope <- coef(linear_fit)[["speed"]]
spark_intercept <- coef(linear_fit)[["(Intercept)"]]

# Plot
sc_cars %>%
  select(speed, dist) %>%
  collect() %>%
  ggplot(aes(dist, speed)) +
  geom_point(aes(speed, dist), size = 2, alpha = 0.5) +
  geom_abline(aes(slope = spark_slope,
                  intercept = spark_intercept),
              color = "red"
              ) +
  labs(x = "speed",
       y = "dist",
       title = "Linear Regression: dist ~ speed",
       subtitle = "Use Spark.ML linear regression to predict dist as a function of speed."
       )

#####-----Disconnect you from spark-------------
spark_disconnect(sc)

