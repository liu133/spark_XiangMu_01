package xiangmu.toos;

public class bin {
    private String status;
    private String message;

    public bin(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public bin() {
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "bin{" +
                "status='" + status + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
