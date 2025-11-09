package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "strconv"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
    rdb *redis.Client
}

func main() {
    addr := getenv("BIND_ADDR", ":8080")
    redisAddr := getenv("REDIS_ADDR", "redis:6379")
    redisPass := os.Getenv("REDIS_PASSWORD")
    db := getInt(getenv("REDIS_DB", "0"), 0)

    rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPass, DB: db})
    if err := pingRedis(rdb); err != nil {
        log.Printf("[WARN] Redis ping failed: %v", err)
    }

    s := &Server{rdb: rdb}

    mux := http.NewServeMux()
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); w.Write([]byte("ok")) })
    mux.HandleFunc("/metrics/active_users_5m", s.handleActiveUsers5m)
    mux.HandleFunc("/metrics/top_products_hourly", s.handleTopProductsHourly)
    mux.HandleFunc("/metrics/abandoned_carts/count", s.handleAbandonedCount)

    // Expose Prometheus metrics on a dedicated port
    go func() {
        http.Handle("/metrics", promhttp.Handler())
        if err := http.ListenAndServe(getenv("METRICS_ADDR", ":2112"), nil); err != nil {
            log.Printf("[ERROR] metrics server: %v", err)
        }
    }()

    log.Printf("[INFO] API listening on %s (redis=%s db=%d); metrics on %s", addr, redisAddr, db, getenv("METRICS_ADDR", ":2112"))
    if err := http.ListenAndServe(addr, cors(mux)); err != nil {
        log.Fatal(err)
    }
}

func (s *Server) handleActiveUsers5m(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    val, err := s.rdb.HGet(ctx, "cartpulse:metrics", "active_users_5m").Result()
    if err == redis.Nil {
        writeJSON(w, http.StatusOK, map[string]interface{}{"active_users_5m": 0})
        return
    } else if err != nil {
        writeErr(w, err)
        return
    }
    n, _ := strconv.ParseInt(val, 10, 64)
    writeJSON(w, http.StatusOK, map[string]interface{}{"active_users_5m": n})
}

func (s *Server) handleTopProductsHourly(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    hour := r.URL.Query().Get("hour") // expect yyyyMMddHH (UTC)
    if hour == "" {
        now := time.Now().UTC()
        hour = now.Format("2006010215")
    }
    key := "cartpulse:top_products_hourly:" + hour
    z, err := s.rdb.ZRevRangeWithScores(ctx, key, 0, -1).Result()
    if err != nil {
        writeErr(w, err)
        return
    }
    type item struct {
        ProductID string  `json:"product_id"`
        Amount    float64 `json:"amount"`
    }
    out := make([]item, 0, len(z))
    for _, e := range z {
        pid, _ := e.Member.(string)
        out = append(out, item{ProductID: pid, Amount: e.Score})
    }
    writeJSON(w, http.StatusOK, map[string]interface{}{"hour": hour, "items": out})
}

func (s *Server) handleAbandonedCount(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    val, err := s.rdb.Get(ctx, "cartpulse:abandoned_carts_count").Result()
    if err == redis.Nil {
        writeJSON(w, http.StatusOK, map[string]interface{}{"count": 0})
        return
    } else if err != nil {
        writeErr(w, err)
        return
    }
    n, _ := strconv.ParseInt(val, 10, 64)
    writeJSON(w, http.StatusOK, map[string]interface{}{"count": n})
}

// --- helpers ---

func cors(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*")
        if r.Method == http.MethodOptions {
            w.Header().Set("Access-Control-Allow-Methods", "GET,OPTIONS")
            w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
            w.WriteHeader(http.StatusNoContent)
            return
        }
        next.ServeHTTP(w, r)
    })
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    _ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
    log.Printf("[ERROR] %v", err)
    writeJSON(w, http.StatusInternalServerError, map[string]interface{}{"error": err.Error()})
}

func getenv(k, def string) string {
    if v := os.Getenv(k); v != "" { return v }
    return def
}

func getInt(s string, def int) int {
    n, err := strconv.Atoi(s)
    if err != nil { return def }
    return n
}

func pingRedis(rdb *redis.Client) error {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    return rdb.Ping(ctx).Err()
}
