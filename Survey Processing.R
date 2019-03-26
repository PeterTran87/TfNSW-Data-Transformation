## Set working folder to the folder containing data file
setwd("D:/University of Wollongong/TfNSW")
library(cronbach)

## Read data file into environment
data <- read.csv("TfNSW_Survey result.csv", header = TRUE, sep = ",")
# Get rid of some unnecessary columns
data <- data[,-c(1,7,8,9,23)]

## Set up factors for the first 4 questions
data[,1] <- factor(data[,1], labels = c("Male", "Female"))
data[,2] <- factor(data[,2], labels = c("Under 18", "19-25", "26-35", "36-50", "51-70", "Over 70"))
data[,3] <- factor(data[,3], labels = c("Self-employed", "Employed Full Time", "Employed Part Time", "Job Seeker", "Student", "Homemaker", "Unemployed", "Disable", "Retired"))
data[,4] <- factor(data[,4], labels = c("Everyday", "Only on weekdays", "Occasionally", "Never use PT", "Others"))

## Separate for pie chart
nonCommuters1 <- data[which(data[,4] == levels(data[,4])[4]),]
nonCommuters1 <- nonCommuters1[,c(1,2,3,4)]
commuters1 <- data[which(data[,4] != levels(data[,4])[4]),]

## Covarience of the ratings
# in Q9
q9 <- data.frame(
  col1 = character(),
  col2 = character(),
  covariance = numeric()
)
for(i in 6:11) {
  for (j in (i+1):12) {
    col1 <- names(commuters1)[i]
    col2 <- names(commuters1)[j]
    covariance <- cov(commuters1[,i], commuters1[,j])
    newEntry <- data.frame(col1, col2, covariance)
    q9 <- rbind(q9, newEntry)
    remove(col1)
    remove(col2)
    remove(covariance)
    remove(newEntry)
  }
}
write.csv(q9, file = "Q9_cov.csv")

# in Q10
q10 <- data.frame(
  col1 = character(),
  col2 = character(),
  covariance = numeric()
)
for(i in 13:17) {
  for (j in (i+1):18) {
    col1 <- names(commuters1)[i]
    col2 <- names(commuters1)[j]
    covariance <- cov(commuters1[,i], commuters1[,j])
    newEntry <- data.frame(col1, col2, covariance)
    q10 <- rbind(q10, newEntry)
    remove(col1)
    remove(col2)
    remove(covariance)
    remove(newEntry)
  }
}
write.csv(q10, file = "Q10_cov.csv")

# Clean up
remove(i)
remove(j)

## Reliability of 2 rating questions
q9_reliability <- cronbach(commuters1[,6:12])
q10_reliability <- cronbach(commuters1[,13:18])

## Separate respondents who are not commuting
nonCommuters <- data[which(data[,4] == levels(data[,4])[4]),]
nonCommuters <- nonCommuters[,c(1,2,3,4)]
commuters <- data[which(data[,4] != levels(data[,4])[4]),]

## Set up factors for transport mode
commuters[,5] <- factor(commuters[,5], labels = c("Train", "Bus", "Ferry", "Light Rail"))

## Set up factors for infrastructure and service quality
commuters[,6] <- factor(commuters[,6], labels = c("Worst", "Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good", "Better"))
commuters[,7] <- factor(commuters[,7], labels = c("Worst", "Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good"))
commuters[,8] <- factor(commuters[,8], labels = c("Worst", "Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good", "Better"))
commuters[,9] <- factor(commuters[,9], labels = c("Worst", "Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good", "Better"))
commuters[,10] <- factor(commuters[,10], labels = c("Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good", "Better"))
commuters[,11] <- factor(commuters[,11], labels = c("Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good", "Better"))
commuters[,12] <- factor(commuters[,12], labels = c("Worst", "Worse", "Bad", "Unacceptable", "Below Average", "Above Average", "Acceptable", "Good"))

## Set up factors for transport experience
commuters[,13] <- factor(commuters[,13], labels = c("Most Dissatisfying", "Dissatisfying", "Totally Unacceptable", "A Bit Unacceptable", "Below Average", "Above Average", "A Bit Acceptable", "Totally Acceptable", "Satisfying"))
commuters[,14] <- factor(commuters[,14], labels = c("Most Dissatisfying", "Dissatisfying", "Totally Unacceptable", "A Bit Unacceptable", "Below Average", "Above Average", "A Bit Acceptable", "Totally Acceptable", "Satisfying"))
commuters[,15] <- factor(commuters[,15], labels = c("Most Dissatisfying", "Totally Unacceptable", "A Bit Unacceptable", "Below Average", "Above Average", "A Bit Acceptable", "Totally Acceptable", "Satisfying"))
commuters[,16] <- factor(commuters[,16], labels = c("Most Dissatisfying", "Dissatisfying", "Totally Unacceptable", "A Bit Unacceptable", "Below Average", "Above Average", "A Bit Acceptable", "Totally Acceptable", "Satisfying"))
commuters[,17] <- factor(commuters[,17], labels = c("Dissatisfying", "Totally Unacceptable", "A Bit Unacceptable", "Below Average", "Above Average", "A Bit Acceptable", "Totally Acceptable", "Satisfying"))
commuters[,18] <- factor(commuters[,18], labels = c("Most Dissatisfying", "Dissatisfying", "Totally Unacceptable", "A Bit Unacceptable", "Below Average", "Above Average", "A Bit Acceptable", "Totally Acceptable", "Satisfying"))

## Set up factors for overall PT service
commuters[,19] <- factor(commuters[,19], labels = c("Poor", "Below Average", "Average", "Above Average", "Excellent"))

## Load module (use install packages if modules don't exist)
# install.packages("ggplot2", dependencies = TRUE)
library(ggplot2)
# install.packages("reshape2", dependencies = TRUE)
library(reshape2)

## Preliminary analysis
setwd("D:/University of Wollongong/TfNSW/Survey/Q1.Q5 - Drill Down")
# Gender ratio
jpeg(filename = "Gender ratio.jpeg", width = 480, height = 480, units = "px")
x <- c(table(data[,1])[1], table(data[,1])[2])
pct <- round(100*x/sum(x), 2)
pie(x, labels = pct, main = "Gender ratio",col = rainbow(length(x)))
legend("topright", c("Male", "Female"), cex = 0.8, fill = rainbow(length(x)))
dev.off()
# Age ratio
jpeg(filename = "Age ratio.jpeg", width = 480, height = 480, units = "px")
x <- c(table(data[,2])[1], table(data[,2])[2], table(data[,2])[3], table(data[,2])[4], table(data[,2])[5], table(data[,2])[6])
pct <- round(100*x/sum(x), 2)
pie(x, labels = pct, main = "Age ratio",col = rainbow(length(x)))
legend("topright", c("Under 18", "19-25", "26-35", "36-50", "51-70", "Over 70"), cex = 0.8, fill = rainbow(length(x)))
dev.off()
# Occupation ratio
jpeg(filename = "Occupation ratio.jpeg", width = 720, height = 720, units = "px")
x <- c(table(data[,3])[1], table(data[,3])[2], table(data[,3])[3], table(data[,3])[4], table(data[,3])[5], table(data[,3])[6], table(data[,3])[7], table(data[,3])[8], table(data[,3])[9])
pct <- round(100*x/sum(x), 2)
pie(x, labels = pct, main = "Occupation ratio",col = rainbow(length(x)))
legend("topright", c("Self-employed", "Employed Full Time", "Employed Part Time", "Job Seeker", "Student", "Homemaker", "Unemployed", "Disable", "Retired"), cex = 0.8, fill = rainbow(length(x)))
dev.off()
# Frequency ratio
jpeg(filename = "Frequency ratio.jpeg", width = 480, height = 480, units = "px")
x <- c(table(data[,4])[1], table(data[,4])[2], table(data[,4])[3], table(data[,4])[4], table(data[,4])[5])
pct <- round(100*x/sum(x), 2)
pie(x, labels = pct, main = "Frequency ratio",col = rainbow(length(x)))
legend("topright", c("Everyday", "Only on weekdays", "Occasionally", "Never use PT", "Others"), cex = 0.8, fill = rainbow(length(x)))
dev.off()
# Transport Mode ratio
jpeg(filename = "Transport Mode ratio.jpeg", width = 480, height = 480, units = "px")
x <- c(table(data[,5])[1], table(data[,5])[2], table(data[,5])[3], table(data[,5])[4])
pct <- round(100*x/sum(x), 2)
pie(x, labels = pct, main = "Transport Mode ratio",col = rainbow(length(x)))
legend("topright", c("Train", "Bus", "Ferry", "Light Rail"), cex = 0.8, fill = rainbow(length(x)))
dev.off()
# Clean
rm(x)
rm(pct)

setwd("D:/University of Wollongong/TfNSW/Survey/Q1.Q5 - Drill Across")
## Plot gender vs. occupation
jpeg(filename = "Gender vs. Occupation.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters) + 
  ggtitle("Gender vs. Occupation") + 
  geom_bar(aes(Q3), stat = "count", fill = "darkred") + 
  labs(x = "Age", y = "Count") + 
  facet_wrap(~Q1) + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

## Plot age range vs. occupation
jpeg(filename = "Age vs. Occupation.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters) + 
  ggtitle("Age vs. Occupation") + 
  geom_bar(aes(Q3), stat = "count", fill = "darkred") + 
  labs(x = "Age", y = "Count") + 
  facet_wrap(~Q2) + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

## Plot occupation vs. frequency
jpeg(filename = "Occupation vs. Frequency.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters) + 
  ggtitle("Occupation vs. Frequency") + 
  geom_bar(aes(Q4), stat = "count", fill = "darkred") + 
  labs(x = "Frequency", y = "Count") + 
  facet_wrap(~Q3) + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

## Plot occupation vs. transport mode
jpeg(filename = "Occupation vs. Transport Mode.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters) + 
  ggtitle("Occupation vs. Transport Mode") + 
  geom_bar(aes(Q5), stat = "count", fill = "darkred") + 
  labs(x = "Frequency", y = "Count") + 
  facet_wrap(~Q3) + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

setwd("D:/University of Wollongong/TfNSW/Survey/Q9 - Infrastructure and Service Quality")
## Plot a bar chart and boxplot for Q9
jpeg(filename = "Rating on Offered Information.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Offered Information") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.A * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Cleanliness.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Cleanliness") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.B * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Available Assistance.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Available Assistance") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.C * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Refreshment Facility Variety.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Refreshment Facility Variety") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.D * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Refreshment Facility Access.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Refreshment Facility Access") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.E * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Toilet Facility.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Toilet Facility") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.F * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Waiting Benches at Stops or Stations.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Waiting Benches at Stops or Stations") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q9.G * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

setwd("D:/University of Wollongong/TfNSW/Survey/Q10 - Transport Experience")
## Plot a bar chart and boxplot for Q10
jpeg(filename = "Rating on Staff Friendliness.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Staff Friendliness") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q10.A * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Comfort During Travel.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Comfort During Travel") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q10.B * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Temperature.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Temperature") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q10.C * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on On-board Information.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on On-board Information") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q10.D * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Cleanliness on the Vehicle.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Cleanliness on the Vehicle") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q10.E * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

jpeg(filename = "Rating on Other Customers' Behaviours.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Other Customers' Behaviours") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q10.F * 5), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./5, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

setwd("D:/University of Wollongong/TfNSW/Survey/Q12 - Overall PT Service")
## Plot a bar chart and boxplot for Q12
jpeg(filename = "Rating on Overall PT Experience.jpeg", width = 1366, height = 720, units = "px")
ggplot(commuters1) + 
  ggtitle("Rating on Overall PT Experience") + 
  geom_bar(aes(Q3), stat = "count", fill = "green") + 
  geom_boxplot(aes(x = Q3, y = Q12 * 10), fill='#A4A4A4', color="red") + 
  scale_y_continuous(sec.axis = sec_axis(~./10, name = "Rating")) + 
  labs(x = "Occupation", y = "Count") + 
  theme(text = element_text(size = 17.5), axis.text.x = element_text(angle=45, hjust=1))
dev.off()

setwd("D:/University of Wollongong/TfNSW/Survey/Q9.Q10 - Ratings vs. Occupation")
## Group occupation for infrastructure and service quality rating
jpeg(filename = "Infrastructure and Service Quality Rating vs. Occupation.jpeg", width = 1366, height = 720, units = "px")
df1 <- melt(commuters1[,c(3,6,7,8,9,10,11,12)], id.vars = "Q3")
names(df1) <- c("Occupation", "Aspects", "Ratings")
df1[,2] <- factor(df1[,2], labels = c("Info. offered", "Cleanliness", "Avai. assist.", "Refr. fac. var.", "Refr. fac. acc.", "Toilet fac.", "Wait. benches"))
ggplot(df1) + 
  geom_boxplot(aes(x = Aspects, y = Ratings), fill = rainbow(63), color = "black") + 
  labs(title = "Infrastructure and Service Quality Rating vs. Occupation") + 
  theme(text = element_text(size = 16), axis.text.x = element_text(angle=0, hjust=1)) + 
  coord_flip() + 
  facet_wrap(~Occupation)
dev.off()

## Group occupation for transport experience rating
jpeg(filename = "Transport Experience Rating vs. Occupation.jpeg", width = 1366, height = 720, units = "px")
df2 <- melt(commuters1[,c(3,13,14,15,16,17,18)], id.vars = "Q3")
names(df2) <- c("Occupation", "Aspects", "Ratings")
df2[,2] <- factor(df2[,2], labels = c("Staff friendliness", "Comfort dur. travel", "Temperature", "On-board info.", "Cleanliness on veh.", "Other cus.s' behav."))
ggplot(df2) + 
  geom_boxplot(aes(x = Aspects, y = Ratings), fill = rainbow(54), color = "black") + 
  labs(title = "Transport Experience Rating vs. Occupation") + 
  theme(text = element_text(size = 16), axis.text.x = element_text(angle=0, hjust=1)) + 
  coord_flip() + 
  facet_wrap(~Occupation)
dev.off()

# Clean
rm(df1)
rm(df2)

## Save data (specify the directory correctly)
save.image("D:/University of Wollongong/TfNSW/Survey/Survey_Data.RData")
