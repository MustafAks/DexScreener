package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    // Konfigürasyon değerleri
    private static String BOT_TOKEN;
    private static long CHAT_ID;
    private static String API_URL;
    private static int GEM_SCORE_THRESHOLD;
    private static long NOTIFICATION_COOLDOWN_PERIOD;
    private static double MARKET_CAP_MULTIPLIER;
    private static long MAIN_LOOP_DELAY;
    private static int MAX_RETRY_COUNT;
    private static long INITIAL_RETRY_DELAY;
    private static long RETRY_DELAY_INCREMENT;
    private static String DB_URL;

    private static DatabaseManager dbManager;
    private static TelegramNotifier telegramNotifier;

    public static void main(String[] args) {
        loadConfig();
        setupDatabase();
        telegramNotifier = new TelegramNotifier(BOT_TOKEN);

        try {
            while (true) {
                JSONArray jsonResponse = fetchTokensWithRetry();
                if (jsonResponse == null) {
                    LOGGER.warning("API'ye ulaşılamadı, bir sonraki döngüde tekrar denenecek...");
                    Thread.sleep(MAIN_LOOP_DELAY);
                    continue;
                }

                processTokens(jsonResponse);

                // İstenirse buradaki bekleme süresi değiştirilebilir.
                // Şu an 8 saat bekliyor.
                Thread.sleep(MAIN_LOOP_DELAY);
            }
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Ana döngü kesintiye uğradı", e);
            Thread.currentThread().interrupt();
        }
    }

    private static void loadConfig() {
        BOT_TOKEN = "6482508265:AAEDUmyCM-ygU7BVO-txyykS7cKn5URspmY";
        CHAT_ID = 1692398446;
        API_URL = "https://api.dexscreener.com/token-boosts/latest/v1";
        GEM_SCORE_THRESHOLD = 5;
        NOTIFICATION_COOLDOWN_PERIOD = 24L * 60L * 60L * 1000L; // 24 saat
        MARKET_CAP_MULTIPLIER = 5.0;

        // 10 saniyede bir istek atmak için 10.000 ms bekliyoruz
        MAIN_LOOP_DELAY = 10000; // 10 saniye

        MAX_RETRY_COUNT = 5;
        INITIAL_RETRY_DELAY = 5000;
        RETRY_DELAY_INCREMENT = 5000;
        DB_URL = "jdbc:sqlite:C:\\Users\\musta\\.local\\share\\DBeaverData\\workspace6\\.metadata\\sample-database-sqlite-1\\Chinook.db";
    }


    private static void setupDatabase() {
        dbManager = new DatabaseManager(DB_URL);
        dbManager.initializeDatabase();
    }

    private static JSONArray fetchTokensWithRetry() {
        JSONArray jsonResponse = null;
        int attempt = 0;
        long currentDelay = INITIAL_RETRY_DELAY;

        while (attempt < MAX_RETRY_COUNT && jsonResponse == null) {
            attempt++;
            try {
                jsonResponse = fetchLatestTokens(API_URL);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "API isteği başarısız oldu. Deneme: " + attempt + " Hata: " + e.getMessage(), e);
                if (attempt < MAX_RETRY_COUNT) {
                    LOGGER.info("Yeniden denemeden önce " + (currentDelay / 1000) + " saniye bekleniyor...");
                    try {
                        Thread.sleep(currentDelay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    currentDelay += RETRY_DELAY_INCREMENT;
                }
            }
        }
        return jsonResponse;
    }

    private static void processTokens(JSONArray jsonResponse) {
        // Ortalamaları al (örnek olarak son 100 token)
        DatabaseManager.TokenAverages averages = dbManager.getAverages();

        Set<String> processedTokens = new HashSet<>();
        for (int i = 0; i < jsonResponse.length(); i++) {
            JSONObject token = jsonResponse.getJSONObject(i);

            String tokenAddress = token.optString("tokenAddress", "Bilinmiyor");
            if (processedTokens.contains(tokenAddress)) {
                continue;
            }

            JSONObject tokenDetails = fetchTokenDetails(tokenAddress);
            if (tokenDetails == null) {
                LOGGER.info("Token detay bilgisi alınamadı: " + tokenAddress);
                continue;
            }

            TokenData data = parseTokenDetails(tokenDetails);
            if (data == null) {
                continue;
            }

            int gemScore = GemScorer.calculateGemScore(data, averages.avgMarketCap, averages.avgLiquidity, averages.avgVolume);
            boolean isGem = gemScore >= GEM_SCORE_THRESHOLD;

            if (isGem) {
                handleGemToken(tokenAddress, data, gemScore);
            }

            processedTokens.add(tokenAddress);

            // İstatistikler için token_metrics tablonuza veri ekleyerek zamanla ortalama hesaplarını iyileştirebilirsiniz.
            dbManager.insertTokenMetrics(tokenAddress, data.marketCap, data.liquidityUsd, data.volume24h, System.currentTimeMillis());
        }
    }

    private static void handleGemToken(String tokenAddress, TokenData data, int gemScore) {
        long currentTimeMillis = System.currentTimeMillis();
        DatabaseManager.TokenInfo tokenInfo = dbManager.getTokenInfo(tokenAddress);

        boolean shouldNotify = false;
        if (tokenInfo == null) {
            // İlk kez gem
            shouldNotify = true;
        } else {
            boolean cooldownExpired = (currentTimeMillis - tokenInfo.getLastNotifiedTime()) >= NOTIFICATION_COOLDOWN_PERIOD;
            boolean bigMarketCapJump = (tokenInfo.getLastNotifiedMarketCap() > 0)
                    && (data.marketCap > tokenInfo.getLastNotifiedMarketCap() * MARKET_CAP_MULTIPLIER);
            if (cooldownExpired || bigMarketCapJump) {
                shouldNotify = true;
            }
        }

        if (shouldNotify) {
            String gemMessage = String.format(
                    "Gem Token Adayı!\n" +
                            "DEX ID: %s\n" +
                            "Token Symbol: %s\n" +
                            "Token Name: %s\n" +
                            "Fiyat (USD): %s\n" +
                            "Fiyat Değişimi (24h): %.2f\n" +
                            "Likidite (USD): %.2f\n" +
                            "Hacim (24 Saat): %.2f\n" +
                            "Market Cap: %d\n" +
                            "İşlem Sayısı (24 Saat): %d\n" +
                            "Puan: %d/%d\n" +
                            "URL: %s\n",
                    data.dexId, data.baseTokenSymbol, data.baseTokenName, data.priceUsd, data.priceChange24h,
                    data.liquidityUsd, data.volume24h, data.marketCap, data.txns24h, gemScore, GEM_SCORE_THRESHOLD, data.pairUrl
            );
            telegramNotifier.sendMessage(CHAT_ID, gemMessage);

            if (tokenInfo == null) {
                dbManager.insertTokenInfo(tokenAddress, currentTimeMillis, data.marketCap);
            } else {
                dbManager.updateTokenInfo(tokenAddress, currentTimeMillis, data.marketCap);
            }
        }
    }

    private static JSONArray fetchLatestTokens(String apiUrl) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(15000);
        conn.connect();

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new Exception("HTTP Durum Kodu: " + responseCode);
        }

        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            StringBuilder inline = new StringBuilder();
            while (scanner.hasNext()) {
                inline.append(scanner.nextLine());
            }
            return new JSONArray(inline.toString());
        }
    }

    private static JSONObject fetchTokenDetails(String tokenAddress) {
        try {
            String detailUrl = "https://api.dexscreener.com/latest/dex/tokens/" + tokenAddress;
            HttpURLConnection conn = (HttpURLConnection) new URL(detailUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(15000);
            conn.connect();

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                LOGGER.warning("Detay API isteği başarısız oldu. HTTP Durum Kodu: " + responseCode);
                return null;
            }

            try (Scanner scanner = new Scanner(conn.getInputStream())) {
                StringBuilder inline = new StringBuilder();
                while (scanner.hasNext()) {
                    inline.append(scanner.nextLine());
                }
                JSONObject jsonResponse = new JSONObject(inline.toString());
                if (jsonResponse.has("pairs")) {
                    JSONArray pairs = jsonResponse.getJSONArray("pairs");
                    if (pairs.length() > 0) {
                        return pairs.getJSONObject(0);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "fetchTokenDetails hata: " + e.getMessage(), e);
        }
        return null;
    }

    private static TokenData parseTokenDetails(JSONObject tokenDetails) {
        String dexId = tokenDetails.optString("dexId", "Bilinmiyor");
        String priceUsd = tokenDetails.optString("priceUsd", "0.0");

        double priceChange24h = 0.0;
        if (tokenDetails.optJSONObject("priceChange") != null) {
            priceChange24h = tokenDetails.optJSONObject("priceChange").optDouble("h24", 0.0);
        }

        double liquidityUsd = 0.0;
        if (tokenDetails.optJSONObject("liquidity") != null) {
            liquidityUsd = tokenDetails.optJSONObject("liquidity").optDouble("usd", 0.0);
        }

        double volume24h = 0.0;
        if (tokenDetails.optJSONObject("volume") != null) {
            volume24h = tokenDetails.optJSONObject("volume").optDouble("h24", 0.0);
        }

        String baseTokenSymbol = "Bilinmiyor";
        String baseTokenName = "Bilinmiyor";
        if (tokenDetails.optJSONObject("baseToken") != null) {
            baseTokenSymbol = tokenDetails.optJSONObject("baseToken").optString("symbol", "Bilinmiyor");
            baseTokenName = tokenDetails.optJSONObject("baseToken").optString("name", "Bilinmiyor");
        }

        int marketCap = tokenDetails.optInt("marketCap", 0);
        String pairUrl = tokenDetails.optString("url", "Bilinmiyor");
        long pairCreatedAt = tokenDetails.optLong("pairCreatedAt", 0);

        int txns24h = 0;
        if (tokenDetails.optJSONObject("txns") != null && tokenDetails.optJSONObject("txns").optJSONObject("h24") != null) {
            JSONObject txns24hObj = tokenDetails.optJSONObject("txns").optJSONObject("h24");
            int buys24h = txns24hObj.optInt("buys", 0);
            int sells24h = txns24hObj.optInt("sells", 0);
            txns24h = buys24h + sells24h;
        }

        return new TokenData(dexId, priceUsd, priceChange24h, liquidityUsd, volume24h, baseTokenSymbol, baseTokenName, marketCap, pairUrl, pairCreatedAt, txns24h);
    }

    // TokenData Sınıfı
    static class TokenData {
        String dexId;
        String priceUsd;
        double priceChange24h;
        double liquidityUsd;
        double volume24h;
        String baseTokenSymbol;
        String baseTokenName;
        int marketCap;
        String pairUrl;
        long pairCreatedAt;
        int txns24h;

        public TokenData(String dexId, String priceUsd, double priceChange24h, double liquidityUsd, double volume24h,
                         String baseTokenSymbol, String baseTokenName, int marketCap, String pairUrl, long pairCreatedAt, int txns24h) {
            this.dexId = dexId;
            this.priceUsd = priceUsd;
            this.priceChange24h = priceChange24h;
            this.liquidityUsd = liquidityUsd;
            this.volume24h = volume24h;
            this.baseTokenSymbol = baseTokenSymbol;
            this.baseTokenName = baseTokenName;
            this.marketCap = marketCap;
            this.pairUrl = pairUrl;
            this.pairCreatedAt = pairCreatedAt;
            this.txns24h = txns24h;
        }

        public long getTokenAgeMs() {
            return System.currentTimeMillis() - pairCreatedAt;
        }
    }

    // Ağırlıklı puanlama ve istatistiksel kıyaslama sınıfı
    static class GemScorer {
        private static final long THIRTY_DAYS_MS = 30L * 24L * 60L * 60L * 1000L;

        // Ağırlıklar
        private static double WEIGHT_PRICE_CHANGE = 1.0;
        private static double WEIGHT_LIQUIDITY = 1.0;
        private static double WEIGHT_LOW_MARKET_CAP = 2.0;
        private static double WEIGHT_VOLUME_OVER_MARKETCAP = 2.0;
        private static double WEIGHT_NEW_TOKEN = 1.0;
        private static double WEIGHT_HIGH_TXNS = 1.0;

        // İstatistik bazlı ek puanlar
        private static double WEIGHT_LIQUIDITY_ABOVE_AVG = 1.5;
        private static double WEIGHT_VOLUME_ABOVE_AVG = 1.5;

        public static int calculateGemScore(TokenData data, double avgMarketCap, double avgLiquidity, double avgVolume) {
            double score = 0.0;

            if (data.priceChange24h > 0) score += WEIGHT_PRICE_CHANGE;
            if (data.liquidityUsd > 5000 && data.liquidityUsd < 50000) score += WEIGHT_LIQUIDITY;
            if (data.marketCap > 0 && data.marketCap < 50000) score += WEIGHT_LOW_MARKET_CAP;
            if (data.volume24h > data.marketCap && data.marketCap > 0) score += WEIGHT_VOLUME_OVER_MARKETCAP;
            if (data.pairCreatedAt > 0 && data.getTokenAgeMs() < THIRTY_DAYS_MS) score += WEIGHT_NEW_TOKEN;
            if (data.txns24h > 50) score += WEIGHT_HIGH_TXNS;

            // İstatistiksel karşılaştırmalar
            if (avgLiquidity > 0 && data.liquidityUsd > avgLiquidity * 2) {
                score += WEIGHT_LIQUIDITY_ABOVE_AVG;
            }

            if (avgVolume > 0 && data.volume24h > avgVolume * 2) {
                score += WEIGHT_VOLUME_ABOVE_AVG;
            }

            return (int)Math.round(score);
        }
    }

    // Telegram bildirim sınıfı
    static class TelegramNotifier {
        private String botToken;

        public TelegramNotifier(String botToken) {
            this.botToken = botToken;
        }

        public void sendMessage(long chatId, String message) {
            try {
                String telegramApiUrl = String.format("https://api.telegram.org/bot%s/sendMessage", botToken);
                HttpURLConnection conn = (HttpURLConnection) new URL(telegramApiUrl).openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                conn.setConnectTimeout(10000);
                conn.setReadTimeout(15000);

                message = message.replace("\"", "\\\"");

                String payload = String.format("{\"chat_id\":%d,\"text\":\"%s\"}", chatId, message);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(payload.getBytes());
                    os.flush();
                }

                int responseCode = conn.getResponseCode();
                if (responseCode != 200) {
                    LOGGER.warning("Telegram mesajı gönderilemedi. HTTP Durum Kodu: " + responseCode);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Telegram'a mesaj gönderirken hata: " + e.getMessage(), e);
            }
        }
    }

    // Veritabanı yöneticisi
    static class DatabaseManager {
        private String dbUrl;

        public DatabaseManager(String dbUrl) {
            this.dbUrl = dbUrl;
        }

        public void initializeDatabase() {
            String createTableSQL = "CREATE TABLE IF NOT EXISTS token_info (" +
                    "tokenAddress TEXT PRIMARY KEY," +
                    "lastNotifiedTime LONG," +
                    "initialMarketCap LONG," +
                    "lastNotifiedMarketCap LONG" +
                    ")";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Veritabanı oluşturulurken hata: " + e.getMessage(), e);
            }

            // token_metrics tablosu oluşturmayı unutmayın.
            // Aşağıdaki tablo örnek:
            // CREATE TABLE IF NOT EXISTS token_metrics (
            //    id INTEGER PRIMARY KEY AUTOINCREMENT,
            //    tokenAddress TEXT,
            //    marketCap LONG,
            //    liquidityUsd DOUBLE,
            //    volume24h DOUBLE,
            //    createdAt LONG
            // );
        }

        public TokenInfo getTokenInfo(String tokenAddress) {
            String sql = "SELECT lastNotifiedTime, initialMarketCap, lastNotifiedMarketCap FROM token_info WHERE tokenAddress = ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, tokenAddress);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        long lastNotifiedTime = rs.getLong("lastNotifiedTime");
                        long initialMarketCap = rs.getLong("initialMarketCap");
                        long lastNotifiedMarketCap = rs.getLong("lastNotifiedMarketCap");
                        return new TokenInfo(tokenAddress, lastNotifiedTime, initialMarketCap, lastNotifiedMarketCap);
                    }
                }
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Veritabanından bilgi alınırken hata: " + e.getMessage(), e);
            }
            return null;
        }

        public void insertTokenInfo(String tokenAddress, long lastNotifiedTime, long marketCap) {
            String sql = "INSERT INTO token_info(tokenAddress, lastNotifiedTime, initialMarketCap, lastNotifiedMarketCap) VALUES(?,?,?,?)";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, tokenAddress);
                pstmt.setLong(2, lastNotifiedTime);
                pstmt.setLong(3, marketCap);
                pstmt.setLong(4, marketCap);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Veritabanına eklerken hata: " + e.getMessage(), e);
            }
        }

        public void updateTokenInfo(String tokenAddress, long lastNotifiedTime, long lastNotifiedMarketCap) {
            String sql = "UPDATE token_info SET lastNotifiedTime = ?, lastNotifiedMarketCap = ? WHERE tokenAddress = ?";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setLong(1, lastNotifiedTime);
                pstmt.setLong(2, lastNotifiedMarketCap);
                pstmt.setString(3, tokenAddress);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Veritabanında güncellerken hata: " + e.getMessage(), e);
            }
        }

        public void insertTokenMetrics(String tokenAddress, long marketCap, double liquidityUsd, double volume24h, long createdAt) {
            String sql = "INSERT INTO token_metrics(tokenAddress, marketCap, liquidityUsd, volume24h, createdAt) VALUES(?,?,?,?,?)";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, tokenAddress);
                pstmt.setLong(2, marketCap);
                pstmt.setDouble(3, liquidityUsd);
                pstmt.setDouble(4, volume24h);
                pstmt.setLong(5, createdAt);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "token_metrics tablosuna eklerken hata: " + e.getMessage(), e);
            }
        }

        public TokenAverages getAverages() {
            // LIMIT kaldırıldı, böylece tüm tablodan ortalama alınır.
            String sql = "SELECT AVG(marketCap) AS avgMarketCap, AVG(liquidityUsd) AS avgLiquidity, AVG(volume24h) AS avgVolume FROM token_metrics";
            try (Connection conn = DriverManager.getConnection(dbUrl);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        double avgMarketCap = rs.getDouble("avgMarketCap");
                        double avgLiquidity = rs.getDouble("avgLiquidity");
                        double avgVolume = rs.getDouble("avgVolume");
                        return new TokenAverages(avgMarketCap, avgLiquidity, avgVolume);
                    }
                }
            } catch (SQLException e) {
                LOGGER.log(Level.WARNING, "Ortalama değerleri alırken hata: " + e.getMessage(), e);
            }
            return new TokenAverages(0,0,0);
        }

    // Basit bir POJO sınıf
    static class TokenInfo {
        private String tokenAddress;
        private long lastNotifiedTime;
        private long initialMarketCap;
        private long lastNotifiedMarketCap;

        public TokenInfo(String tokenAddress, long lastNotifiedTime, long initialMarketCap, long lastNotifiedMarketCap) {
            this.tokenAddress = tokenAddress;
            this.lastNotifiedTime = lastNotifiedTime;
            this.initialMarketCap = initialMarketCap;
            this.lastNotifiedMarketCap = lastNotifiedMarketCap;
        }

        public String getTokenAddress() {
            return tokenAddress;
        }

        public long getLastNotifiedTime() {
            return lastNotifiedTime;
        }

        public long getInitialMarketCap() {
            return initialMarketCap;
        }

        public long getLastNotifiedMarketCap() {
            return lastNotifiedMarketCap;
        }
    }

    static class TokenAverages {
        double avgMarketCap;
        double avgLiquidity;
        double avgVolume;

        public TokenAverages(double avgMarketCap, double avgLiquidity, double avgVolume) {
            this.avgMarketCap = avgMarketCap;
            this.avgLiquidity = avgLiquidity;
            this.avgVolume = avgVolume;
        }
    }


}
