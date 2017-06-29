import java.io.Serializable;

public class Transaction implements Serializable{
    private Integer transactionId;
    private Integer transactions_clientId;
    private Integer transactions_productId;

    public Transaction(Integer transactionId, Integer transactions_clientId, Integer transactions_productId) {
        this.transactionId = transactionId;
        this.transactions_clientId = transactions_clientId;
        this.transactions_productId = transactions_productId;
    }

    public Integer getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Integer transactionId) {
        this.transactionId = transactionId;
    }

    public Integer getTransactions_clientId() {
        return transactions_clientId;
    }

    public void setTransactions_clientId(Integer transactions_clientId) {
        this.transactions_clientId = transactions_clientId;
    }

    public Integer getTransactions_productId() {
        return transactions_productId;
    }

    public void setTransactions_productId(Integer transactions_productId) {
        this.transactions_productId = transactions_productId;
    }
}
