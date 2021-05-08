package lazysort

import (
	"bufio"
	"math/big"
	"os"
)

type LazySort struct {
	FileName string
	OutChan  chan * big.Int
	maxMemorySize int
	tempFp *os.File
	dataLen int
	dataByteSize int
}

func (p * LazySort) numToBytes(num *big.Int, byteData [] byte) {
	tempDataByte := num.Bytes()
		
	if len(tempDataByte) > p.dataByteSize {
		panic("Data Too Big.")
	}

	diff_len := p.dataByteSize - len(tempDataByte)

	for j := 0; j < p.dataByteSize; j++ {
		if j >= diff_len {
			byteData[j] = tempDataByte[j - diff_len]
		} else {
			byteData[j] = 0
		}
	}
}

func (p * LazySort) readTempFileUInt(index, length int) [] *big.Int {
	result := make([] *big.Int, length)
	readData := make([] byte, p.dataByteSize * length)
	
	p.tempFp.ReadAt(readData, int64(index * p.dataByteSize))

	for i := 0; i < length; i++ {
		tempNum := new(big.Int)
		tempNum.SetBytes(readData[i * p.dataByteSize: (i + 1) * p.dataByteSize])
		result[i] = tempNum
	}

	return result
}

func (p * LazySort) writeTempFileUInt(index int, data [] *big.Int) {
	writeData := make([] byte, p.dataByteSize * len(data))
	for i := 0; i < len(data); i++ {
		// binary.LittleEndian.PutUint64(writeData[i * p.dataByteSize: (i + 1) * p.dataByteSize], data[i])
		p.numToBytes(data[i], writeData[i * p.dataByteSize: (i + 1) * p.dataByteSize])
	}

	p.tempFp.WriteAt(writeData, int64(index * p.dataByteSize))
}

func (p * LazySort) swapTempFileUInt(index1, index2 int) {
	data1 := p.readTempFileUInt(index1, 1)[0]
	data2 := p.readTempFileUInt(index2, 1)[0]
	p.writeTempFileUInt(index2, [] * big.Int {data1})
	p.writeTempFileUInt(index1, [] * big.Int {data2})
}

func (p * LazySort) createBufferFile() {
	fp, err := os.Open(p.FileName)
	dataLen := 0

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
		// readItem, err := strconv.ParseUint(fileScanner.Text(), 10, 64)
		readItem := new(big.Int)
		readItem.SetString(fileScanner.Text(), 10)

		if err != nil {
			panic(err)
		}
		
		dataLen += 1
		bs := make([]byte, p.dataByteSize)
		p.numToBytes(readItem, bs)
		
		_, err = Tempfp.Write(bs)
		if err != nil {
			panic(err)
		}
	}

	p.dataLen = dataLen
	p.tempFp = Tempfp
}

func NewLazySort(filename string, dataByteSize int) *LazySort {
	p := new(LazySort)
	p.FileName = filename
	p.OutChan = make(chan * big.Int)
	// p.data = readDataFromFile(filename)
	p.maxMemorySize = 1024
	p.dataByteSize = dataByteSize

	p.createBufferFile()
	go p.sortIt(0, p.dataLen)
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
		// if leftData > beginData {
		if leftData.Cmp(beginData) > 0 {
			p.swapTempFileUInt(left, right)
			right --
		} else {
			left ++
		}
	}

	p.swapTempFileUInt(begin, right)

	p.sortIt(begin, right)
	p.OutChan <- beginData
	p.sortIt(right + 1, end)
}

func (p * LazySort) sortIt(begin, end int) {
	if end - begin < p.maxMemorySize {
		p.sortMemory(begin, end)
	} else {
		p.sortExternal(begin, end)
	}

	if (begin == 0 && end == p.dataLen) {
		close(p.OutChan)
	}
}

func (p * LazySort) sortMemoryWork(data [] * big.Int) {
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
		// if data[left] > data[0] {
		if data[left].Cmp(data[0]) > 0 {
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
}

func (p * LazySort) sortMemory(begin, end int) {
	data := p.readTempFileUInt(begin, end - begin)
	p.sortMemoryWork(data)
	p.writeTempFileUInt(begin, data)
}
