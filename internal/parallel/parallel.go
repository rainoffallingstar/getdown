package parallel

import (
	"context"
	"runtime"
	"sync"
)

func Normalize(workers, count int) int {
	if count <= 0 {
		return 1
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 2 {
			workers = 2
		}
		if workers > 8 {
			workers = 8
		}
	}
	if workers > count {
		workers = count
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

func ForEach(ctx context.Context, workers, count int, fn func(context.Context, int) error) error {
	if count <= 0 {
		return nil
	}
	workers = Normalize(workers, count)
	if workers == 1 {
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(ctx, i); err != nil {
				return err
			}
		}
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	indexes := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error

	worker := func() {
		defer wg.Done()
		for i := range indexes {
			if ctx.Err() != nil {
				return
			}
			if err := fn(ctx, i); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker()
	}

	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			break
		}
		indexes <- i
	}
	close(indexes)
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}
