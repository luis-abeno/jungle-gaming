package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	targets     = flag.String("targets", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma separated instance URLs")
	keycloak    = flag.String("keycloak", "http://localhost:8180", "Keycloak base URL")
	duration    = flag.Duration("duration", 30*time.Second, "test duration")
	concurrency = flag.Int("concurrency", 32, "concurrent clients")
	wallets     = flag.Int("wallets", 50, "wallets to spread operations over")
	replayRatio = flag.Float64("replays", 0.1, "fraction of requests that resend an earlier operation")
	rate        = flag.Int("rate", 0, "total requests per second, 0 for as fast as possible")
)

type wallet struct{ id, player string }

type sample struct {
	latency time.Duration
	status  int
}

func main() {
	flag.Parse()
	urls := strings.Split(*targets, ",")
	internal := token("wallet-internal")
	provider := token("provider-a")
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency}}

	ws := make([]wallet, *wallets)
	for i := range ws {
		player := fmt.Sprintf("0192f28f-5dc0-7d58-bdb2-%012d", rand.Int64N(1e12))
		body := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"1000000.00","currency":"BRL"}}`, player)
		status, resp := do(client, "POST", urls[i%len(urls)]+"/wallets", internal, "", body)
		if status != http.StatusCreated {
			fail("open wallet: %d %s", status, resp)
		}
		var w struct{ ID string }
		json.Unmarshal(resp, &w)
		ws[i] = wallet{w.ID, player}
	}

	run := strconv.FormatInt(time.Now().Unix(), 36)
	var (
		mu      sync.Mutex
		samples []sample
		seq     atomic.Int64
		errs    atomic.Int64
		stop    = time.Now().Add(*duration)
		wg      sync.WaitGroup
	)
	var ticks <-chan time.Time
	if *rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(*rate))
		defer t.Stop()
		ticks = t.C
	}
	start := time.Now()
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			var local []sample
			for time.Now().Before(stop) {
				if ticks != nil {
					<-ticks
				}
				n := seq.Add(1)
				if n > 10 && rand.Float64() < *replayRatio {
					n = rand.Int64N(n-1) + 1
				}
				w := ws[n%int64(len(ws))]
				ext := fmt.Sprintf("load-%s-%d", run, n)
				kind, amount := "BET", "1.00"
				if n%4 == 0 {
					kind, amount = "WIN", "0.50"
				}
				body := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"r-%d","gameId":"load","kind":%q,"money":{"amount":%q,"currency":"BRL"}}`,
					ext, w.player, w.id, n, kind, amount)
				t0 := time.Now()
				status, _ := do(client, "POST", urls[int(n)%len(urls)]+"/wagering/transactions", provider, "provider-a:"+ext, body)
				if status == 0 {
					errs.Add(1)
				}
				local = append(local, sample{time.Since(t0), status})
			}
			mu.Lock()
			samples = append(samples, local...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	elapsed := time.Since(start)

	lat := make([]time.Duration, len(samples))
	codes := map[int]int{}
	for i, s := range samples {
		lat[i] = s.latency
		codes[s.status]++
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(p*float64(len(lat))))]
	}

	fmt.Printf("instances     %d\n", len(urls))
	fmt.Printf("duration      %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("concurrency   %d\n", *concurrency)
	fmt.Printf("wallets       %d\n", len(ws))
	fmt.Printf("requests      %d (%.0f req/s)\n", len(samples), float64(len(samples))/elapsed.Seconds())
	fmt.Printf("latency       p50 %s  p95 %s  p99 %s  max %s\n", pct(.50), pct(.95), pct(.99), pct(1))
	fmt.Printf("status codes  %v\n", codes)
	fmt.Printf("client errors %d\n", errs.Load())

	var conflicts, retries, lag, pending, replays float64
	for _, u := range urls {
		m := scrape(client, u)
		conflicts += m.sum("wallet_concurrency_conflicts_total")
		retries += m.sum("wallet_retries_total")
		replays += m.sum("wager_duplicates_total")
		lag = max(lag, m.sum("outbox_lag_seconds"))
		pending = max(pending, m.sum("outbox_pending_events"))
	}
	fmt.Printf("replays       %.0f\n", replays)
	fmt.Printf("conflicts     %.0f (retries %.0f)\n", conflicts, retries)
	fmt.Printf("outbox        pending %.0f, lag %.2fs at last scrape\n", pending, lag)

	drainStart := time.Now()
	for {
		pending = 0
		for _, u := range urls {
			pending = max(pending, scrape(client, u).sum("outbox_pending_events"))
		}
		if pending == 0 || time.Since(drainStart) > time.Minute {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("outbox drain  %s after load (stats refresh every 5s)\n", time.Since(drainStart).Round(time.Second))

	inconsistent := 0
	for i, w := range ws {
		_, resp := do(client, "POST", urls[i%len(urls)]+"/wallets/"+w.id+"/reconciliation", internal, "", "")
		var r struct{ Consistent bool }
		json.Unmarshal(resp, &r)
		if !r.Consistent {
			inconsistent++
		}
	}
	fmt.Printf("reconciliation %d/%d wallets consistent\n", len(ws)-inconsistent, len(ws))
	if inconsistent > 0 {
		os.Exit(1)
	}
}

func do(c *http.Client, method, u, tok, key, body string) (int, []byte) {
	req, _ := http.NewRequest(method, u, bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func token(client string) string {
	resp, err := http.PostForm(*keycloak+"/realms/wallet/protocol/openid-connect/token", url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {client + "-secret"},
	})
	if err != nil {
		fail("token: %v", err)
	}
	defer resp.Body.Close()
	var t struct {
		AccessToken string `json:"access_token"`
	}
	json.NewDecoder(resp.Body).Decode(&t)
	if t.AccessToken == "" {
		fail("no token for %s", client)
	}
	return t.AccessToken
}

type metrics map[string]float64

func (m metrics) sum(prefix string) float64 {
	var total float64
	for k, v := range m {
		if k == prefix || strings.HasPrefix(k, prefix+"{") {
			total += v
		}
	}
	return total
}

func scrape(c *http.Client, base string) metrics {
	out := metrics{}
	resp, err := c.Get(base + "/metrics")
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err == nil {
			out[line[:i]] = v
		}
	}
	return out
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
