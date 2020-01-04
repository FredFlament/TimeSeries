# TimeSeries
Tools to handle time series as seriously as possible for real time data processing in Go.

Time Series are organized as slices of DataUnits, easily extendible. There is only one data per date/time because in IoT
it is not advised to consider constant and/or regular time of data arrival before ad hoc processing.
