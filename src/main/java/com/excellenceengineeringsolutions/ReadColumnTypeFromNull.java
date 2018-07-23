

package com.excellenceengineeringsolutions;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;

public class ReadColumnTypeFromNull
{

    public void transaction(){
        final com.google.cloud.spanner.DatabaseClient client = null;//mHome.getSpannerClientProvider().getDatabaseClient();
        client.readWriteTransaction().run(new com.google.cloud.spanner.TransactionRunner.TransactionCallable<Void>()
        {
            @Nullable
            @Override
            public Void run(com.google.cloud.spanner.TransactionContext transactionContext) throws Exception
            {
                Statement.Builder statementBuilder = Statement.newBuilder("select rowid from X where ");
                statementBuilder.bind("").to(1);
                List<Long> rowIds = new LinkedList<Long>();
                com.google.cloud.spanner.ResultSet rs = transactionContext.executeQuery(statementBuilder.build());

                while(rs.next()){
                    rowIds.add(rs.getLong(0));
                }

                Mutation.WriteBuilder mutation = Mutation.newInsertOrUpdateBuilder("");

                transactionContext.buffer(mutation.build());


                return null;
            }
        });
    }

}
