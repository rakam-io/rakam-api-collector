package io.rakam.presto.connector.custom;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DoubleType;
import com.google.inject.name.Named;
import io.rakam.presto.connector.AbstractDatabaseHandler;
import org.rakam.analysis.JDBCPoolDataSource;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CustomDatabaseHandler extends AbstractDatabaseHandler {
    @Inject
    public CustomDatabaseHandler(@Named("metadata.store.jdbc") JDBCPoolDataSource metadataStore) {
        super(metadataStore);
    }

    @Override
    public Inserter insert(String projectName, String eventType, List<ColumnMetadata> eventProperties) {
        return new Inserter() {
            @Override
            public void addPage(Page page) {
                for (int i = 0; i < eventProperties.size(); i++) {
                    ColumnMetadata property = eventProperties.get(i);
                    Block block = page.getBlock(i);
                    if(property.getType() == DoubleType.DOUBLE) {
                        // the `value` is the first value of property in our micro-batch
                        double value = DoubleType.DOUBLE.getDouble(block, 0);
                    }
                }
            }

            @Override
            public CompletableFuture commit() {
                // if the future is completed, the checkpoint will be executed
                return CompletableFuture.completedFuture(null);
            }
        };
    }
}
