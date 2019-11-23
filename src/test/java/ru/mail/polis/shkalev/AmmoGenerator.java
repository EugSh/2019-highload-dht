package ru.mail.polis.shkalev;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class AmmoGenerator {
    private static final Logger log = LoggerFactory.getLogger(AmmoGenerator.class);
    private static final int VALUE_LEN = 256;
    private static final Random RANDOM = new Random();
    private static final float DEFAULT_REP_PROB = 0.1f;
    private static final float ALL_REPEAT = 1f;//для того, чтобы все ключи генерировались из списка старых ключей

    private static String randomKey() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    private static String randomKey(final float repeatProbability, @NotNull final MyList<String> oldKeys) {
        final float repeat = RANDOM.nextFloat();
        if (Float.compare(repeat, repeatProbability) <= 0 && oldKeys.size() != 0) {
            return oldKeys.get(RANDOM.nextInt(oldKeys.size()));
        }
        return randomKey();
    }

    private static byte[] randomValue(final int length) {
        final byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void putTo(@NotNull final OutputStream out,
                              @NotNull final String key,
                              final byte[] value) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("Content-Length: " + value.length + "\r\n");
            writer.write("\r\n");
        }
        request.write(value);
        out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        out.write(" put\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(out);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    private static void getTo(@NotNull final OutputStream out,
                              @NotNull final String key) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("\r\n");
        }
        out.write(Integer.toString(request.size()).getBytes(StandardCharsets.US_ASCII));
        out.write(" get\n".getBytes(StandardCharsets.US_ASCII));
        request.writeTo(out);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    private static void task1(final int count, @NotNull final String dir) throws IOException {
        try (final FileOutputStream fPut = new FileOutputStream(dir + "/put1.txt")) {
            for (int i = 0; i < count; i++) {
                final String key = randomKey();
                final byte[] value = randomValue(VALUE_LEN);
                putTo(fPut, key, value);
            }
        }
    }

    private static void task2(final int count, @NotNull final String dir) throws IOException {
        try (final FileOutputStream out = new FileOutputStream(dir + "/put2.txt")) {
            final MyList<String> oldKeys = ArrayMyList.create(count);
            for (int i = 0; i < count; i++) {
                final String key = randomKey(DEFAULT_REP_PROB, oldKeys);
                oldKeys.add(key);
                final byte[] value = randomValue(VALUE_LEN);
                putTo(out, key, value);
            }
        }
    }

    private static void task3(final int count, @NotNull final String dir) throws IOException {
        try (final FileOutputStream fPut = new FileOutputStream(dir + "/putForGet3.txt");
             final FileOutputStream fGet = new FileOutputStream(dir + "/get3.txt")) {
            final MyList<String> oldKeys = ArrayMyList.create(count);
            for (int i = 0; i < count; i++) {
                final String key = randomKey();
                oldKeys.add(key);
                final byte[] value = randomValue(VALUE_LEN);
                putTo(fPut, key, value);
            }
            for (int i = 0; i < count; i++) {
                final String key = randomKey(ALL_REPEAT, oldKeys);
                getTo(fGet, key);
            }
        }
    }

    private static void task4(final int count, @NotNull final String dir) throws IOException {
        try (final FileOutputStream fPut = new FileOutputStream(dir + "/putForGet4.txt");
             final FileOutputStream fGet = new FileOutputStream(dir + "/get4.txt")) {
            final MyList<String> oldKeys = FixedQueue.create((int) (count * DEFAULT_REP_PROB));//в очереди будут последние 10% ключей
            for (int i = 0; i < count; i++) {
                final String key = randomKey();
                oldKeys.add(key);
                final byte[] value = randomValue(VALUE_LEN);
                putTo(fPut, key, value);
            }
            for (int i = 0; i < count; i++) {
                final String key = randomKey(ALL_REPEAT, oldKeys);
                getTo(fGet, key);
            }
        }
    }

    private static void task5(final int count, @NotNull final String dir) throws IOException {
        try (final FileOutputStream out = new FileOutputStream(dir + "/putGet5.txt")) {
            final MyList<String> oldKeys = ArrayMyList.create(count);
            int countGet = 0;
            for (int i = 0; i < count; i++) {
                if (RANDOM.nextBoolean() && oldKeys.size() != 0) {
                    countGet++;
                    final String key = randomKey(ALL_REPEAT, oldKeys);
                    getTo(out, key);
                } else {
                    final String key = randomKey();
                    oldKeys.add(key);
                    final byte[] value = randomValue(VALUE_LEN);
                    putTo(out, key, value);
                }
            }
            log.info("count of get = [{}]", countGet);
        }
    }

    public static void main(String[] args) throws IOException {
        int count = 540030;
        String dir = "ammo";
        String task = "5";
        if (args.length != 3) {
            System.out.println("Default ammo file directory "
                    + System.getProperty("user.dir")
                    + "/"
                    + dir
                    + " and default count of ammo "
                    + count
                    + "\n or usage: \n\tjava -cp build/classes/java/test ru.mail.polis.<name>.AmmoGenerator <dir> <count> <№ task>");
        } else {
            dir = args[0];
            count = Integer.parseInt(args[1]);
            task = args[2];
        }
        switch (task) {
            case "1":
                task1(count, dir);
                break;
            case "2":
                task2(count, dir);
                break;
            case "3":
                task3(count, dir);
                break;
            case "4":
                task4(count, dir);
                break;
            case "5":
                task5(count, dir);
                break;
            default:
                System.out.println("Unsupported task " + task);
        }
    }
}
