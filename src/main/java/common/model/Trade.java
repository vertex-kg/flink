package common.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public class Trade implements Serializable {
    private Long id;
    private Long clientId;
    private Long productId;

    private String clientName;
    private String clientParent;
    private String productName;

    public Trade(Long id, Long clientId, Long productId) {
        this.id = id;
        this.clientId = clientId;
        this.productId = productId;
    }

    public Long getId() {
        return id;
    }

    public Long getClientId() {
        return clientId;
    }

    public Long getProductId() {
        return productId;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getClientParent() {
        return clientParent;
    }

    public void setClientParent(String clientParent) {
        this.clientParent = clientParent;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Trade trade = (Trade) o;

        return new EqualsBuilder()
                .append(id, trade.id)
                .append(clientId, trade.clientId)
                .append(productId, trade.productId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(clientId)
                .append(productId)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "Trade{" +
                "id=" + id +
                ", clientId=" + clientId +
                ", productId=" + productId +
                ", clientName='" + clientName + '\'' +
                ", clientParent='" + clientParent + '\'' +
                ", productName='" + productName + '\'' +
                '}';
    }
}
