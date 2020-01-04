package timeseries

import (
	"fmt"
	"gonum.org/v1/gonum/stat"
	"time"
)
// Collection of Timeseries. The terms are self explanatory. When a raw ("Original") timeseries
// is cleaned we have to store the resulting processes, the leftout data, and only the "resampled" series
// with a regular time interval is generally used for statistic computations. The original timeseries is never
// transformed. Therefore, if several operations of cleaning are made on a ts, they compose themselves one on each other
type TsContainer struct {
	Original  TimeSeries
	Cleaned   TimeSeries
	Resampled TimeSeries
	Rejected  TimeSeries
}


func (tsc *TsContainer) DeviceLimitsCleaning(min,max float64){
	tsc.SliceCleaned(min,max,"DeviceLimit ("+fmt.Sprintf("%.2f",min)+","+fmt.Sprintf("%.2f",max)+") - "+time.Now().String())

}

// when resetting the cleaned ts to zero, the resampled series is also reset. A copy of the original is directly
// inserted in cleaned
func (tsc *TsContainer) ResetCleanedSeries(){
	tsc.Resampled.Reset()
	tsc.Cleaned.Reset()
	tsc.Cleaned=tsc.Original
}

// Clean following indicated percentile
func (tsc *TsContainer) PercCleaning(perc float64) {
	cleanedts:= &tsc.Cleaned
	cleanedts.SortMeasAsc()
	dt:= cleanedts.MeasToArr()
	min:=stat.Quantile(perc,1,dt,nil)
	max:=stat.Quantile((1-perc),1,dt,nil)
	tsc.SliceCleaned(min,max,"Percentile ("+fmt.Sprintf("%.2f",min)+","+fmt.Sprintf("%.2f",max)+") - "+time.Now().String())
}
// Clean the cleaned timeseries following zscore technique. Save leftover data into leftover
func (tsc *TsContainer) ZscoreCleaning(lvl float64){
	cleanedts:= &tsc.Cleaned
	cleanedts.SortMeasAsc()
	dt:= cleanedts.MeasToArr()
	dtmean:=stat.Mean(dt,nil)
	dtstd:=stat.StdDev(dt,nil)
	min:=(dtmean-dtstd)*lvl
	max:=(dtmean+dtstd)*lvl
	tsc.SliceCleaned(min,max,"zScore at "+fmt.Sprintf("%.2f",lvl)+ "("+fmt.Sprintf("%.2f",min)+","+fmt.Sprintf("%.2f",max)+") - "+time.Now().String())
}

func (tsc *TsContainer) Downsampling(freqq string, stat string) {
	resampled:= &tsc.Resampled
	resampled.Reset()
	tsc.Cleaned.SortChronAsc()
	var freq, timeunit = InterpretDurationParam(freqq)
	fmt.Println("freq: ", freq, "\t timeunit: ", timeunit)
	datemin := tsc.Cleaned.DataSeries[0].Chron
	datemax := tsc.Cleaned.DataSeries[len(tsc.Cleaned.DataSeries)-1].Chron
	fmt.Println("datemin: ", datemin, "\t datemax: ", datemax)
	mainant := roundedStartTime(datemin, freqq)
	mainant = AddDurationParam(mainant, freqq)
	fmt.Println("première data: ", tsc.Cleaned.DataSeries[0].Chron)
	fmt.Println("borne: ", mainant)
	for i := 0; i < (len(tsc.Cleaned.DataSeries) - 1); {
		sum := 0.0
		var vec []float64
		var obsmin, obsmax float64
		for tsc.Cleaned.DataSeries[i].Chron.Before(mainant) && i < (len(tsc.Cleaned.DataSeries)-1) {
			sum += tsc.Cleaned.DataSeries[i].Meas
			vec = append(vec, tsc.Cleaned.DataSeries[i].Meas)
			i++
		}
		obsmin, obsmax = Bounds(vec)
		var obslast float64
		var du DataUnit
		if len(vec)!=0{
			obslast = vec[len(vec)-1]
			switch stat {
			case "avg":
				du = DataUnit{
					Chron: mainant,
					Meas:  Mean(vec),
				}
			case "max":
				du = DataUnit{
					Chron: mainant,
					Meas:  obsmax,
				}
			case "min":
				du = DataUnit{
					Chron: mainant,
					Meas:  obsmin,
				}
			case "last":
				du = DataUnit{
					Chron: mainant,
					Meas:  obslast,
				}
			default:
				du = DataUnit{
					Chron: mainant,
					Meas: Nihil,
				}
			}
		}else {
			obslast = Nihil
			du = DataUnit{
				Chron: mainant,
				Meas:  Nihil,  //math.NaN(),
			}

		}
		resampled.AddDataUnit(du)
		mainant = AddDurationParam(mainant, freqq)
		for (i< len(tsc.Cleaned.DataSeries)-2)&& mainant.Before(tsc.Cleaned.DataSeries[i+1].Chron)==true {
			du=DataUnit{
				Chron:mainant,
				Meas:Nihil,
			}
			resampled.AddDataUnit(du)
			mainant = AddDurationParam(mainant, freqq)
		}
		i++
	}
}
// Slice the cleaned timeseries from the most extreme value, above max and under min. The removed data
// are inserted in the leftover timeseries with a stamp.
func (tsc *TsContainer) SliceCleaned(min,max float64,cause string){
	cleanedts:= &tsc.Cleaned
	tsc.Cleaned.SortMeasAsc()
	countlow:=0
	counthigh:=0
	for i:=0;i<len(cleanedts.DataSeries)-1 && cleanedts.DataSeries[i].Meas<min;i++{
			countlow+=1
			tsc.Rejected.AddDataUnit(tsc.Cleaned.DataSeries[i])
			tsc.Rejected.DataSeries[len(tsc.Rejected.DataSeries)-1].Origin=cause
	}
	for i:=(len(cleanedts.DataSeries)-1);i>=0 && cleanedts.DataSeries[i].Meas>max;i--{
			counthigh+=1
			tsc.Rejected.AddDataUnit(tsc.Cleaned.DataSeries[i])
			tsc.Rejected.DataSeries[len(tsc.Rejected.DataSeries)-1].Origin=cause

	}
	counthigh= len(cleanedts.DataSeries)-counthigh
	cleanedts.DataSeries=cleanedts.DataSeries[countlow:counthigh+1]
	cleanedts.Complete()
	cleanedts.PrintSummaryStat()
}

