package io.rakam.presto.connector;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.inject.name.Named;
import io.rakam.presto.DatabaseHandler;
import org.rakam.analysis.JDBCPoolDataSource;
import org.skife.jdbi.v2.DBI;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;

public abstract class AbstractDatabaseHandler implements DatabaseHandler {
    private final MetadataDao dao;

    public AbstractDatabaseHandler(@Named("metadata.store.jdbc") JDBCPoolDataSource metadataStore) {
        DBI dbi = new DBI(metadataStore);
        this.dao = onDemandDao(dbi, MetadataDao.class);
    }

    @Override
    public List<ColumnMetadata> getColumns(String schema, String table) {
        List<ColumnMetadata> tableColumns = dao.listTableColumns(schema, table).stream()
                .map(e -> new ColumnMetadata(e.getColumnName(), e.getDataType())).collect(Collectors.toList());
        if (tableColumns.isEmpty()) {
            throw new IllegalArgumentException("Table doesn't exist");
        }

        return tableColumns;
    }
}
