package com.aliyun.openservices.paifeaturestore.datasource;

import java.io.IOException;

public class HttpException extends IOException {
    private int code;
    private String message;

    public HttpException() {
        super();
    }

    public HttpException(int code, String message) {
        super("Status Code: " + code + " Failed:" + message);
        this.code = code;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
    public int getCode() { return code; }
}
