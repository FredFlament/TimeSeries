// Package aimed at managing timeseries in a IoT context. In real world timeseries do not come with constant interval.
// Downsampling and cleaning are of high importance
package timeseries

import (
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/montanaflynn/stats"
	"gonum.org/v1/gonum/stat"
	"math"
	"os"
	"sort"
	"strconv"
	"text/tabwriter"
	"time"
)
const(
	Uzero=math.SmallestNonzeroFloat64
	Nihil=0.0
)
// A Timeseries is a slice of DataUnit (s). The DataUnit is the smallest piece of information.
// This structure is intended to never separate a measure from its timestamp
// Notice that the difference in time and measure from the preceding dataunit are included as element of a TimeSeries
// Dchron is of particular importance for normalizing time interval.
type DataUnit struct {
	Chron  time.Time
	Dchron time.Duration
	Meas   float64
	Dmeas float64
	Origin string
}
// Slice of Data units with a bunch of other information at the time series level.
type TimeSeries struct {
	DataSeries []DataUnit
	Descr      string
	SumStat SummaryStat
}

// A Summary Statistics field can be stored at the timeseries level to spare some further computation
type SummaryStat struct {
	Dslen  int
	Chmin  time.Time
	Chmax  time.Time
	Chmean time.Duration
	Chstd  time.Duration
	Msmin  float64
	Msmax  float64
	Msmean float64
	Msstd  float64
}

// Method to add Dataunit to a timeseries
func (ts *TimeSeries) AddDataUnit(du DataUnit) {
	ts.DataSeries = append(ts.DataSeries, du)
}
// Sort timeseries  in time ascending order. There is no guarantee that IoT data are transmitted
// in ascending order, for different technical reasons.
func (ts *TimeSeries) SortChronAsc() {
	sort.Slice(ts.DataSeries, func(i, j int) bool {
		return ts.DataSeries[i].Chron.Before(ts.DataSeries[j].Chron)
	})
}
// Sort in chronological descending order.
func (ts *TimeSeries) SortChronDesc (){
	sort.Slice(ts.DataSeries, func(i, j int) bool {
		return ts.DataSeries[i].Chron.After(ts.DataSeries[j].Chron)
	})
}
// Print a TimeSeries in a more ordered way in output terminal
func (ts *TimeSeries) Print(what ...int) {
	fmt.Println(ts.Descr)
	var j,k int
	switch len(what) {
	case 0:
		{
			j = 0
			k = len(ts.DataSeries)
		}
	case 1:
		{
			j = 0
			k = what[1]
		}
	case 2:
		{
			j = what[0]
			k = what[1]
		}
	default:
		{
			j = 0
			k = len(ts.DataSeries)
		}
	}
	w:=new(tabwriter.Writer)
	w.Init(os.Stdout, 5, 0, 3, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "index|\ttime|\tMeasure|\tDelta time|\tDelta Measure\t")
	fmt.Fprintln(w, "-----\t--------------------------|\t----------------------|\t---------------|\t------------------")

	for i:=j;i<k && i<len(ts.DataSeries)-1;i++{
		fmt.Fprintf(w, "%d|\t %v|\t%v|\t%v|\t%v|\t\n",i,ts.DataSeries[i].Chron,ts.DataSeries[i].Meas,ts.DataSeries[i].Dchron,ts.DataSeries[i].Dmeas)
		//fmt.Println(i, ts.DataSeries[i].Chron, "\t", ts.DataSeries[i].Meas,"\t", ts.DataSeries[i].Dchron,"\t", ts.DataSeries[i].Dmeas)
	}
	// Format right-aligned in space-separated columns of minimal width 5
	// and at least one blank of padding (so wider column entries do not
	// touch each other).

	fmt.Fprintln(w)
	w.Flush()
	/*
	for index, element := range ts.DataSeries {
		fmt.Println(index, element.Chron,element.Dchron, element.Meas)
	}*/
}

// Reset TimeSeries to a zero length TimeSeries
func (ts *TimeSeries) Reset(){
	ts.DataSeries=ts.DataSeries[0:0]
}
// Self Explanatory
func (ts *TimeSeries) ComputeSummaryStat(){
	ch:= ts.ChronToArr()
	var sumchdelta float64
	var sumchdelta2 float64
	ts.SumStat.Dslen= len(ts.DataSeries)
	for i:=1;i< ts.SumStat.Dslen;i++{
		sumchdelta+=float64(ts.DataSeries[i].Dchron.Microseconds())
		sumchdelta2+=float64(ts.DataSeries[i].Dchron.Microseconds())*float64(ts.DataSeries[i].Dchron.Microseconds())
	}
	ts.SumStat.Chmin= ts.DataSeries[0].Chron
	ts.SumStat.Chmax= ts.DataSeries[ts.SumStat.Dslen-1].Chron
	fl64chmean:=sumchdelta/float64(ts.SumStat.Dslen-2)
	ts.SumStat.Chmean=time.Duration(fl64chmean*1000)
	fl64chmean2:=sumchdelta2
	fl64std:=fl64chmean2/float64(len(ch))-math.Pow(fl64chmean,2)
	ts.SumStat.Chstd=time.Duration(math.Sqrt(fl64std))
	ms:= ts.MeasToArr()
	ts.SumStat.Msmin,_=stats.Min(ms)
	ts.SumStat.Msmax,_=stats.Max(ms)
	ts.SumStat.Msmean,_=stats.Mean(ms)
	ts.SumStat.Msstd,_=stats.StandardDeviation(ms)
}
func (ts *TimeSeries) PrintSummaryStat(){
	w:=new(tabwriter.Writer)
	w.Init(os.Stdout, 5, 0, 3, ' ', tabwriter.AlignRight)
	fmt.Fprintf(w, "Length| %v|\t\n",ts.SumStat.Dslen)
	fmt.Fprintln(w, "\tChron|\tMeasure|\t")
	fmt.Fprintln(w, "\t-----------------\t-------------\t")
	fmt.Fprintf(w, "Mean|\t (Delta Chron)%v|\t%v|\t\n",ts.SumStat.Chmean,ts.SumStat.Msmean)
	fmt.Fprintf(w, "Min|\t %v|\t%v|\t\n",ts.SumStat.Chmin,ts.SumStat.Msmin)
	fmt.Fprintf(w, "Max|\t %v|\t%v|\t\n", ts.SumStat.Chmax,ts.SumStat.Msmax)
	fmt.Fprintf(w, "StdDev|\t |\t%v|\t\n" ,ts.SumStat.Msstd)

	fmt.Fprintln(w)
	w.Flush()

}

// Sort in ascending order and complete a raw data series with the time interval and summary statistics
func (ts *TimeSeries) Complete(){
	ts.SortChronAsc()
	ts.DataSeries[0].Dchron=0.0
	ts.DataSeries[0].Dmeas=0.0
	for i:=1;i<len(ts.DataSeries)-1;i++{
		ts.DataSeries[i].Dchron= ts.DataSeries[i].Chron.Sub(ts.DataSeries[i-1].Chron)
		ts.DataSeries[i].Dmeas= ts.DataSeries[i].Meas- ts.DataSeries[i-1].Meas
	}
	ts.ComputeSummaryStat()
}
// Modify a TimeSeries to remove extrema of 2 times the Percentile in parameter
func (ts *TimeSeries) CleanedForOutliers(perc float64){
	cleanedts:= ts
	cleanedts.SortMeasAsc()
	dt:= ts.MeasToArr()
	quant5:=stat.Quantile(perc,1,dt,nil)
	quant95:=stat.Quantile(1-perc,1,dt,nil)
	countlow:=0
	counthigh:=0
	for i:=0;i<len(ts.DataSeries)-1;i++{
		if cleanedts.DataSeries[i].Meas<quant5 {
			countlow+=1
		}
		if cleanedts.DataSeries[i].Meas>quant95 {
			counthigh+=1
		}
	}
	counthigh= len(cleanedts.DataSeries)-counthigh
	fmt.Println(quant5,quant95)
	fmt.Println(countlow,counthigh)
	cleanedts.DataSeries=cleanedts.DataSeries[countlow:counthigh+1]
	cleanedts.Complete()
	cleanedts.PrintSummaryStat()
}
// Sort a TimeSeries in ascending order of measure
func (ts *TimeSeries) SortMeasAsc() {
	sort.Slice(ts.DataSeries, func(i, j int) bool {
		return ts.DataSeries[i].Meas< ts.DataSeries[j].Meas
	})
}
func (ts *TimeSeries) OutputTotxt(outputfile string){
	output,err:=os.Create(outputfile)
	defer output.Close()
	if err!=nil{
		return
	}
	for i,_ := range ts.DataSeries{
		fmt.Fprintf(output,"%d \t %v \t %v\n",i,ts.DataSeries[i].Chron,ts.DataSeries[i].Meas,ts.DataSeries[i].Dchron)
	}
}
// Produce a slice from a TimeSeries. Useful for computation requiring arrays. Typically math libraries
func (ts TimeSeries) MeasToArr() []float64{
	var measarr []float64
	for index,_:=range ts.DataSeries{
		measarr=append(measarr, ts.DataSeries[index].Meas)
	}
	return measarr
}
func (ts TimeSeries) ChronToArr() []time.Time{
	var measarr []time.Time
	for index,_:=range ts.DataSeries{
		measarr=append(measarr, ts.DataSeries[index].Chron)
	}
	return measarr
}

// Remove
func DelFast(ts *TimeSeries,i int){
	ts.DataSeries[i]=ts.DataSeries[len(ts.DataSeries)-1]
	//ts.DataSeries[len(ts.DataSeries)-1]=""
	ts.DataSeries=ts.DataSeries[:len(ts.DataSeries)-1]
	ts.SortChronAsc()
}

// Merge two TimeSeries. Return a new TimeSeries
func Merge(tsa *TimeSeries, tsb *TimeSeries) TimeSeries {
	var tsr TimeSeries
	for _, element := range tsa.DataSeries {
		tsr.AddDataUnit(element)
	}
	for _, element := range tsb.DataSeries {
		tsr.AddDataUnit(element)
	}
	return tsr
}
func CleanForNA(obs []float64) []float64 {
	sort.Float64s(obs)
	var cleaned []float64
	startcut:=0
	stopcut:=0
	for i:=0;i< len(obs);i++{
		if obs[i]>0 && startcut==0{
			cleaned=obs
			break}
		if obs[i]>0 && startcut>0 {
			stopcut=i
			cleaned=append(obs[:startcut],obs[stopcut:]...)
			break}
		if obs[i]==Nihil && startcut==0{
			startcut=i
		}
	}

	return cleaned
}
func roundedStartTime(timetoround time.Time, afreqq string) time.Time {
	ff, _ := strconv.Atoi(afreqq[0 : len(afreqq)-1])
	per := afreqq[len(afreqq)-1 : len(afreqq)]
	roundedtime := time.Now()
	switch per {
	case "m":
		roundedtime = timetoround.Truncate(time.Minute * time.Duration(ff))
	case "s":
		roundedtime = timetoround.Truncate(time.Second * time.Duration(ff))
	case "h":
		roundedtime = timetoround.Truncate(time.Hour * time.Duration(ff))
	case "d":
		roundedtime = timetoround.AddDate(0, 0, -ff)
	default:
		roundedtime = timetoround
	}
	return roundedtime
}
func Tmean(obs []float64) (float64,float64,error){
	sum:=0.0
	count:=0.0
	for _,value:=range obs{
		sum += value
		if value!=Nihil{
			count+=1.0
		}
	}
	if count==0.0{
		return 0.0,0.0,errors.New("Population is empty")
	}else{
		return sum/count,count,nil
	}
}
func Mean(xs []float64) float64 {
	if len(xs) == 0.00000 {
		return Nihil
	}
	m := 0.0
	for i, x := range xs {
		m += (x - m) / float64(i+1)
	}
	return m
}
func Bounds(xs []float64) (min float64, max float64) {
	if len(xs) == 0.00000000 {
		return 0.0,0.0 //math.NaN(), math.NaN()
	}
	min, max = xs[0], xs[0]
	for _, x := range xs {
		if x < min {
			min = x
		}
		if x > max {
			max = x
		}
	}
	return
}

func AddDurationParam(start time.Time, freqq string) time.Time {
	/*step,_:=time.ParseDuration(freqq)
	start.Add(step)
	return start
	*/
	var freq, timeunit = InterpretDurationParam(freqq)
	switch timeunit {
	case "s":
		return start.Add(time.Second * time.Duration(freq))
	case "m":
		return start.Add(time.Minute * time.Duration(freq))
	case "h":
		return start.Add(time.Hour * time.Duration(freq))
		//case "d":return start.Add(time*time.Duration(freq))
	default:
		return start
	}
}
func InterpretDurationParam(freqq string) (int, string) {
	freq, _ := strconv.Atoi(freqq[0 : len(freqq)-1])
	timeunit := freqq[len(freqq)-1 : len(freqq)]
	return freq, timeunit
}