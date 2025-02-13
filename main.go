package main

import (
    "bytes"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "runtime"
    "runtime/pprof"
    "runtime/trace"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

var (
    measurementsFile = flag.String("measurements", "", "path to the measurements file")
    cpuprofile       = flag.String("cpuprofile", "", "write cpu profile to `file`")
    memprofile       = flag.String("memprofile", "", "write memory profile to `file`")
    traceprofile     = flag.String("traceprofile", "", "write tarce execution to `file`")
    numprocs         = flag.String("numprocs", "", "the number of processes to start")

    resultChan = make(chan map[string]*cityTemp, 100)
    workerChan = make(chan []byte, 100)
    workerWg   sync.WaitGroup
)

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
        if err = pprof.StartCPUProfile(f); err != nil {
            log.Fatal("could not start CPU profile: ", err)
        }
        defer pprof.StopCPUProfile()
    }

    startTime := time.Now()
    execute(*measurementsFile)
    endTime := time.Now()
    fmt.Println("Elapsed Time:", endTime.Sub(startTime))

    if *memprofile != "" {
        f, err := os.Create("./profiles/" + *memprofile)
        if err != nil {
            log.Fatal("could not create memory profile: ", err)
        }
        defer func(f *os.File) {
            _ = f.Close()
        }(f)
        runtime.GC()
        if err = pprof.WriteHeapProfile(f); err != nil {
            log.Fatal("could not write memory profile: ", err)
        }
    }
}

func execute(filename string) {
    // make execute jobs worker
    makeWorker()

    // read measurements file
    go processFile(filename)

    results := processResult()

    var stringsBuilder strings.Builder
    for _, i := range results {
        stringsBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", i.city, i.min, i.avg, i.max))
    }

    fmt.Printf("{%s}\n", stringsBuilder.String())
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
    var num int
    if *numprocs != "" {
        num, _ = strconv.Atoi(*numprocs)
    }
    if num < 1 {
        num = runtime.NumCPU()
    }
    fmt.Printf("Starting workers:%d\n", num)
    for i := 0; i < num; i++ {
        workerWg.Add(1)
        go worker()
    }
}

func worker() {
    defer workerWg.Done()
    for {
        data, ok := <-workerChan
        if !ok {
            break
        }

        resultChan <- processChunk(data)
    }
}

func processFile(filename string) {
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
        if e != nil {
            if errors.Is(e, io.EOF) {
                break
            }
            panic(e)
        }
        if n == 0 {
            break
        }
        buf = buf[:n]

        lastNewLineIndex := bytes.LastIndex(buf, []byte("\n"))

        toSend := make([]byte, len(leftover)+lastNewLineIndex+1)
        copy(toSend, leftover)
        copy(toSend[len(leftover):], buf[:lastNewLineIndex+1])

        // 将本次读取的数据最后一个 \n 后的数据赋值给 leftover，以备下一轮使用
        leftover = append(leftover[:0], buf[lastNewLineIndex+1:]...)

        workerChan <- toSend
    }

    close(workerChan)
    workerWg.Wait()

    close(resultChan)
}

func resolveLine(line []byte) ([]byte, int64) {
    end := len(line)
    if line[end-1] == '\r' {
        end -= 1
    }

    var temp int64
    var semi int
    var tens int64
    minus := false
    tenths := int64(line[end-1] - '0')
    ones := int64(line[end-3] - '0')

    i := end - 4
    for ; i > 0; i-- {
        if line[i] == ';' {
            semi = i
            break
        } else if line[i] == '-' {
            minus = true
            semi = i - 1
            break
        } else {
            tens = int64(line[i] - '0')
        }
    }

    temp = tens*100 + ones*10 + tenths
    if minus {
        temp = -temp
    }

    return line[:semi], temp
}

func processChunk(chunk []byte) map[string]*cityTemp {
    var offset int
    tmpStats := make(map[string]*cityTemp)
    hash := newHash()
    for i := 0; i < len(chunk); i++ {
        switch chunk[i] {
        case '\n':
            line := chunk[offset:i]
            offset = i + 1
            city, temp := resolveLine(line)

            hashIndex := hash.hashIndex(city)
            hitem := hash.items[hashIndex]
            if hitem.key == nil {
                key := make([]byte, len(city))
                copy(key, city)
                hash.items[hashIndex] = hashitem{
                    key: key,
                    val: &cityTemp{
                        min:   temp,
                        max:   temp,
                        sum:   temp,
                        count: 1,
                    },
                }
                hash.len++
            } else if bytes.Equal(hitem.key, city) {
                stat := hitem.val
                stat.count++
                stat.sum += temp
                if temp > stat.max {
                    stat.max = temp
                }
                if temp < stat.min {
                    stat.min = temp
                }
            }

            if hash.len > len(hash.items)/2 {
                panic("too many items in hash table")
            }
        }
    }

    for _, v := range hash.items {
        if v.key == nil {
            continue
        }
        tmpStats[string(v.key)] = v.val
    }

    return tmpStats
}

func processResult() []collectResult {
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

    collect := make([]collectResult, 0, len(resultTemp))
    for city, v := range resultTemp {
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

    return collect
}

type hashx struct {
    len   int
    items [100000]hashitem
}

type hashitem struct {
    key []byte
    val *cityTemp
}

func newHash() *hashx {
    return new(hashx)
}

const (
    // offset64 FNVa offset basis. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
    offset64 = 14695981039346656037
    // prime64 FNVa prime value. See https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function#FNV-1a_hash
    prime64 = 1099511628211
)

func (h *hashx) Sum64(key []byte) uint64 {
    var hash uint64 = offset64
    for i := 0; i < len(key); i++ {
        hash ^= uint64(key[i])
        hash *= prime64
    }

    return hash
}

func (h *hashx) hashIndex(key []byte) int {
    hashedKey := h.Sum64(key)
    hashIndex := int(hashedKey & uint64(len(h.items)-1))
    return hashIndex
}
