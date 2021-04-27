package lazy_sort

import (
	"bufio"
	"encoding/binary"
	"os"
	"strconv"
)

type LazySort struct {
	FileName string
	// data     [] uint64
	OutChan  chan uint64
	max_size int
	tempFp *os.File
	number_size int
}

func (p * LazySort) readTempFileUInt(index, length int) [] uint64 {
	result := make([] uint64, length)
	readData := make([] byte, 8 * length)
	
	// p.tempFp.Seek(int64(index * 8), 0)

	p.tempFp.ReadAt(readData, int64(index * 8))

	for i := 0; i < length; i++ {
		result[i] = binary.LittleEndian.Uint64(readData[i * 8: (i + 1) * 8])
	}

	return result
}

func (p * LazySort) writeTempFileUInt(index int, data [] uint64) {
	writeData := make([] byte, 8 * len(data))
	for i := 0; i < len(data); i++ {
		binary.LittleEndian.PutUint64(writeData[i * 8: (i + 1) * 8], data[i])
	}
	// p.tempFp.Seek(int64(index * 8), 0)
	p.tempFp.WriteAt(writeData, int64(index * 8))
}

func (p * LazySort) swapTempFileUInt(index1, index2 int) {
	data1 := p.readTempFileUInt(index1, 1)[0]
	data2 := p.readTempFileUInt(index2, 1)[0]
	p.writeTempFileUInt(index2, [] uint64{data1})
	p.writeTempFileUInt(index1, [] uint64{data2})
}

func (p * LazySort) createBufferFile() {
	fp, err := os.Open(p.FileName)
	number_size := 0

	if err != nil {
		panic(err)
	}
	defer fp.Close()

	bufferFile := ".*"
	Tempfp, err := os.CreateTemp("", bufferFile)
	if err != nil {
		panic(err)
	}
	
	fileScanner := bufio.NewScanner(fp)
	for fileScanner.Scan() {
		readItem, err := strconv.ParseUint(fileScanner.Text(), 10, 64)

		if err != nil {
			panic(err)
		}
		
		number_size += 1
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, readItem)
		
		_, err = Tempfp.Write(bs)
		if err != nil {
			panic(err)
		}
	}

	p.number_size = number_size
	p.tempFp = Tempfp
}

func NewLazySort(filename string) *LazySort {
	p := new(LazySort)
	p.FileName = filename
	p.OutChan = make(chan uint64)
	// p.data = readDataFromFile(filename)
	p.max_size = 1024
	p.createBufferFile()
	go p.sortIt(0, p.number_size)
	return p
}

func (p *LazySort) sortExternal(begin, end int) {
	if (begin >= end) {
		return
	}

	if (begin + 1 == end) {
		outData := p.readTempFileUInt(begin, 1)[0]
		p.OutChan <- outData
		return
	}
	
	left := begin + 1
	right := end - 1

	beginData := p.readTempFileUInt(begin, 1)[0]

	for left <= right {
		leftData := p.readTempFileUInt(left, 1)[0]
		// rightData := p.readTempFileUInt(right, 1)[0]
		if leftData > beginData {
			// temp := p.data[left]
			// p.data[left] = p.data[right]
			// p.data[right] = temp
			// p.data[left], p.data[right] = p.data[right], p.data[left]
			p.swapTempFileUInt(left, right)
			right --
		} else {
			left ++
		}
	}

	// p.data[begin], p.data[right] = p.data[right], p.data[begin]

	p.swapTempFileUInt(begin, right)

	p.sortIt(begin, right)
	p.OutChan <- beginData
	p.sortIt(right + 1, end)
}

func (p * LazySort) sortIt(begin, end int) {
	if end - begin < p.max_size {
		p.sortMemory(begin, end)
	} else {
		p.sortExternal(begin, end)
	}

	if (begin == 0 && end == p.number_size) {
		close(p.OutChan)
	}
}

func (p * LazySort) sortMemoryWork(data [] uint64) {
	if (len(data) <= 0) {
		return
	}

	if (len(data) == 1) {
		p.OutChan <- data[0]
		return
	}
	
	left := 1
	right := len(data) - 1

	for left <= right {
		if data[left] > data[0] {
			data[left], data[right] = data[right], data[left]
			right --
		} else {
			left ++
		}
	}

	data[0], data[right] = data[right], data[0]

	// p.sortIt(begin, right)
	if right > 0 {
		p.sortMemoryWork(data[: right])
	}
	
	p.OutChan <- data[right]
	
	if right + 1 < len(data) {
		p.sortMemoryWork(data[right + 1:])
	}
	// p.sortIt(right + 1, end)

	
}

func (p * LazySort) sortMemory(begin, end int) {
	data := p.readTempFileUInt(begin, end - begin)
	p.sortMemoryWork(data)
	p.writeTempFileUInt(begin, data)
}
