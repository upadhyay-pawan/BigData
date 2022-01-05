import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class XLSXTemplate {

    @XLSXField(column = "Column Name")
    private String columnName;

    @XLSXField(column = "Data Type")
    private String dataType;

    @XLSXField(column = "Comment")
    private String comment;

    @XLSXField(column = "Table Name")
    private String tableName;

    @XLSXField(column = "Action")
    private String action;

    @XLSXField(column = "Alter Column Name")
    private String alterColumnName;

    @XLSXField(column = "Alter Option")
    private String alterOption;

    @XLSXField(column = "Alter Data Type")
    private String alterDataType;

}