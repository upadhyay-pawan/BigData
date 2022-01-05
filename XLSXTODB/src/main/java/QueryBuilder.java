import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {
    private StringBuilder sqlQuery = new StringBuilder();
    private String action;

    public List<String> BuildQuery(List<XLSXTemplate> list) {
        List<String> queryList = new ArrayList<>();

        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getAction().equals("Create")) {
                action = list.get(i).getAction();
                sqlQuery.append("Create table IF NOT EXISTS testdb." + list.get(i).getTableName() + "(\n");
            } else if (list.get(i).getAction().equals("Alter")) {
                action = "Alter";
                sqlQuery.append("Alter table testdb." + list.get(i).getTableName() + " " + list.get(i).getAlterOption() + " columns (");
            }

            switch (action) {
                case "Create":
                    sqlQuery.append(list.get(i).getColumnName() + " " +
                            list.get(i).getDataType() + " " +
                            " COMMENT " + "'" +
                            list.get(i).getComment() + "'" + ",");
                    break;
                case "Alter":
                    if (list.get(i).getAlterOption().equalsIgnoreCase("Add")) {
                        sqlQuery.append(list.get(i).getAlterColumnName() + " " + list.get(i).getAlterDataType() + " ,");
                    }
                    break;
                default:
                    System.out.println("invalid action");
                    return null;
            }
            if (list.get(i).getAlterDataType().equals("END")){
                sqlQuery.deleteCharAt(sqlQuery.length() - 1).append(")");
                System.out.println(sqlQuery);
                queryList.add(sqlQuery.toString());
                sqlQuery.setLength(0);
            }
        }

        return queryList;
    }
}
