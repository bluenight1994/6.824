package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//  registerChan yields a string for each worker, containing worker's RPC address

	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(task_id int, n_other int) {
			// Decrement the counter when the goroutine completes
			defer wg.Done()
			// goroutine implementation
			for {
				cur_worker := <-registerChan

				var args DoTaskArgs

				// fill in DoTaskArgs
				args.JobName = jobName
				args.File = mapFiles[task_id]
				args.Phase = phase
				args.TaskNumber = task_id
				args.NumOtherPhase = n_other
				
				ok := call(cur_worker, "Worker.DoTask", &args, new(struct{}))
				if ok {
					go func() {
						registerChan <- cur_worker
					}()
					break
				}
			}
		}(i, n_other)
	}

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}