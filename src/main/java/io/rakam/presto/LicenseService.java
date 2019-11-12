package io.rakam.presto;

import com.amazonaws.util.StringInputStream;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.kms.v1.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import okhttp3.*;
import okio.Okio;
import org.rakam.util.JsonHelper;

import javax.inject.Inject;
import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LicenseService {

    private final ScheduledExecutorService scheduler;
    private final LicenseConfig config;
    private Instant licenseSuccessfullyCheckedAt;

    @Inject
    public LicenseService(LicenseConfig config) {
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.licenseSuccessfullyCheckedAt = Instant.now();
        this.config = config;

        if (config.getServiceAccountJson() == null) {
            throw new IllegalStateException("Service Account JSON is required for the license server.");
        }
        if (config.getKeyName() == null) {
            throw new IllegalStateException("License key-name is required for the license server.");
        }
        scheduler.scheduleAtFixedRate(this::isLicenseValid, 45, 60, TimeUnit.SECONDS);

        if (!isLicenseValid()) {
            throw new IllegalStateException("initial license check failed.");
        }
    }

    public boolean isLicenseValid() {
        CryptoKeyPathName cryptoKeyPathName = CryptoKeyPathName.of(config.getProject(), config.getKeyRingLocation(), config.getKeyRing(), config.getKeyName());

        String cipherText;
        try {
            cipherText = generateEncryptedLicenseMessage(config.getServiceAccountJson(), cryptoKeyPathName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            String encode = JsonHelper.encode(ImmutableMap.of("keyName", config.getKeyName(), "cipherText", cipherText));

            OkHttpClient client = new OkHttpClient();

            RequestBody body = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), encode);
            Request request = new Request.Builder()
                    .url("https://services.rakam.io/license/check")
                    .post(body)
                    .build();
            try (Response response = client.newCall(request).execute()) {
                int status = response.code();
                if (status == 200) {
                    licenseSuccessfullyCheckedAt = Instant.now();
                }

                return status == 200;
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateEncryptedLicenseMessage(String serviceAccountJSON, CryptoKeyPathName cryptoKeyPathName) throws IOException {
        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(new StringInputStream(serviceAccountJSON));
        KeyManagementServiceSettings settings = KeyManagementServiceSettings
                .newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
        KeyManagementServiceClient client = KeyManagementServiceClient.create(settings);
        // 🐣 Easter-egg encryption message.
        ByteString plaintext = ByteString.copyFrom("Komşunun tavuğu komşuya kaz görünür dersen\n" +
                "Kaz gelen yerden tavuğu esirgemezsen\n" +
                "Bu kafayla bir baltaya sap olamazsın ama\n" +
                "Gün gelir sapın ucuna olursun kazma", StandardCharsets.UTF_8);

        ByteString encryptedMessage = client.encrypt(cryptoKeyPathName, plaintext).getCiphertext();
        client.close();

        return BaseEncoding.base64().encode(encryptedMessage.toByteArray());
    }
}
