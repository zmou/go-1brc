package main

import (
    "bufio"
    "bytes"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "runtime"
    "runtime/pprof"
    "runtime/trace"
    "sort"
    "strings"
    "sync"
    "time"
)

var measurementsFile = flag.String("measurements", "", "path to the measurements file")
var resultChan = make(chan map[string]*cityTemp, 10000)
var workerChan = make(chan []string, 1000)
var byteChan = make(chan []byte, 200)
var workerWg sync.WaitGroup
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var traceprofile = flag.String("traceprofile", "", "write tarce execution to `file`")

func main() {
    flag.Parse()

    if *measurementsFile == "" {
        panic("measurements file is required")
    }

    if *traceprofile != "" {
        f, err := os.Create("./profiles/" + *traceprofile)
        if err != nil {
            log.Fatal("could not create trace execution profile: ", err)
        }
        defer func(f *os.File) {
            _ = f.Close()
        }(f)
        _ = trace.Start(f)
        defer trace.Stop()
    }

    if *cpuprofile != "" {
        f, err := os.Create("./profiles/" + *cpuprofile)
        if err != nil {
            log.Fatal("could not create CPU profile: ", err)
        }
        defer func(f *os.File) {
            _ = f.Close()
        }(f)
        if err := pprof.StartCPUProfile(f); err != nil {
            log.Fatal("could not start CPU profile: ", err)
        }
        defer pprof.StopCPUProfile()
    }

    startTime := time.Now()
    collect := process(*measurementsFile)

    fmt.Println(collect)
    fmt.Println("Elapsed Time:", time.Since(startTime))

    if *memprofile != "" {
        f, err := os.Create("./profiles/" + *memprofile)
        if err != nil {
            log.Fatal("could not create memory profile: ", err)
        }
        defer func(f *os.File) {
            _ = f.Close()
        }(f)
        runtime.GC()
        if err := pprof.WriteHeapProfile(f); err != nil {
            log.Fatal("could not write memory profile: ", err)
        }
    }
}

func process(filename string) string {
    // make process jobs worker
    makeWorker()

    // read measurements file
    go readFile2(filename)

    results := processResult()
    collect := make([]collectResult, 0, len(results))
    for city, v := range results {
        collect = append(collect, collectResult{
            city: city,
            min:  float64(v.min) / 10,
            max:  float64(v.max) / 10,
            avg:  float64(v.sum) / float64(v.count) / 10,
        })
    }

    sort.SliceStable(collect, func(i, j int) bool {
        return collect[i].city < collect[j].city
    })

    var stringsBuilder strings.Builder
    for _, i := range collect {
        stringsBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", i.city, i.min, i.avg, i.max))
    }

    return stringsBuilder.String()
}

type cityTemp struct {
    min   int64
    max   int64
    sum   int64
    count int64
}

type collectResult struct {
    city string
    min  float64
    max  float64
    avg  float64
}

func makeWorker() {
    num := runtime.NumCPU()
    for i := 0; i < num; i++ {
        workerWg.Add(1)
        go worker()
    }
}

func worker() {
    defer workerWg.Done()
    for {
        data, ok := <-byteChan
        if !ok {
            break
        }

        resultChan <- processChunk(data)
    }
}

func resolveLine(line string) (string, int64) {
    end := len(line)
    tenths := int32(line[end-1] - '0')
    ones := int32(line[end-3] - '0') // line[end-2] is '.'
    var temp int32
    var semicolon int
    if line[end-4] == ';' { // positive N.N temperature
        temp = ones*10 + tenths
        semicolon = end - 4
    } else if line[end-4] == '-' { // negative -N.N temperature
        temp = -(ones*10 + tenths)
        semicolon = end - 5
    } else {
        tens := int32(line[end-4] - '0')
        if line[end-5] == ';' { // positive NN.N temperature
            temp = tens*100 + ones*10 + tenths
            semicolon = end - 5
        } else { // negative -NN.N temperature
            temp = -(tens*100 + ones*10 + tenths)
            semicolon = end - 6
        }
    }
    station := line[:semicolon]

    return station, int64(temp)
}

func readFile(filename string) {
    file, err := os.Open(filename)
    if err != nil {
        panic(err)
    }
    defer func(file *os.File) {
        _ = file.Close()
    }(file)

    buf := make([]byte, 64*1024*1024)
    scanner := bufio.NewScanner(file)
    scanner.Buffer(buf, 64*1024*1024)
    chunkSize := 512 * 1024
    chunkStream := make([]string, 0, chunkSize)
    i := 0
    for scanner.Scan() {
        line := scanner.Text()
        i++
        if i >= chunkSize {
            workerChan <- chunkStream
            chunkStream = chunkStream[:0]
            i = 0
        } else {
            chunkStream = append(chunkStream, line)
        }
    }
    if len(chunkStream) > 0 {
        workerChan <- chunkStream
    }

    close(workerChan)
    workerWg.Wait()

    close(resultChan)
}

func readFile2(filename string) {
    file, err := os.Open(filename)
    if err != nil {
        panic(err)
    }
    defer func(file *os.File) {
        _ = file.Close()
    }(file)

    bufSize := 64 * 1024 * 1024 // 64MB
    buf := make([]byte, bufSize)
    leftover := make([]byte, 0, bufSize)

    for {
        n, e := file.Read(buf)
        if n == 0 {
            break
        }
        if e != nil {
            if err == io.EOF {
                break
            }
            panic(e)
        }
        buf = buf[:n]

        lastNewLineIndex := bytes.LastIndex(buf, []byte("\n"))

        toSend := make([]byte, 0, len(leftover)+lastNewLineIndex+1)
        toSend = append(toSend, leftover...)
        toSend = append(toSend, buf[:lastNewLineIndex+1]...)

        // 将本次读取的数据最后一个 \n 后的数据赋值给 leftover，以备下一轮使用
        leftover = append(leftover[:0], buf[lastNewLineIndex+1:]...)

        byteChan <- toSend
    }

    close(byteChan)
    workerWg.Wait()

    close(resultChan)
}

func processChunk(chunk []byte) map[string]*cityTemp {
    chunkStr := string(chunk)
    var start int
    tmpStats := make(map[string]*cityTemp)

    for i := 0; i < len(chunkStr); i++ {
        switch chunkStr[i] {
        case '\n':
            line := chunkStr[start:i]
            end := len(line)
            if line[end-1] == '\r' {
                line = line[:end-1]
            }
            city, temp := resolveLine(line)
            start = i + 1

            stat, exists := tmpStats[city]
            if !exists {
                tmpStats[city] = &cityTemp{
                    min:   temp,
                    max:   temp,
                    sum:   temp,
                    count: 1,
                }
            } else {
                stat.count++
                stat.sum += temp
                if temp > stat.max {
                    stat.max = temp
                }
                if temp < stat.min {
                    stat.min = temp
                }
            }
        }
    }

    return tmpStats
}

func processResult() map[string]*cityTemp {
    var resultTemp = make(map[string]*cityTemp)

    for t := range resultChan {
        for city, temp := range t {
            if val, o := resultTemp[city]; !o {
                resultTemp[city] = temp
            } else {
                val.count += temp.count
                if temp.max > val.max {
                    val.max = temp.max
                }
                if temp.min < val.min {
                    val.min = temp.min
                }
                val.sum += temp.sum
            }
        }
    }

    return resultTemp
}
