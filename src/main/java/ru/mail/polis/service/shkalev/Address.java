package ru.mail.polis.service.shkalev;

import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Address implements Comparable<Address> {
    private final String host;
    private final int port;

    Address(@NotNull final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    Address(@NotNull final String address) {
        final String regex = "http.?://.*:";
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(address);
        if (matcher.find()) {
            this.host = address.substring(0, matcher.end() - 1);
            this.port = Integer.parseInt(address.substring(matcher.end()));
        } else {
            throw new IllegalArgumentException("Invalid address. (http://host:port)");
        }
    }

    String getHost() {
        return host;
    }

    int getPort() {
        return port;
    }

    @Override
    public boolean equals(@NotNull final Object o) {
        if (this == o) return true;
        if (!(o instanceof Address)) return false;
        final Address address = (Address) o;
        return port == address.port && host.equals(address.host);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int compareTo(@NotNull final Address address) {
        return Comparator.comparing(Address::toString).compare(this, address);
    }
}
