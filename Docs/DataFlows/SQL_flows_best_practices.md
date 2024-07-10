# Best practices for SQL data flows

## Only work with data relevant for the current flow run

This increases performance significantly and prevents potentially unwanted interconnections between flow runs.

- Save the `[#flow_run_uid#]` in all intermediate tables and use `WHERE [#flow_run_uid#]` in steps

## Write easily reproducable statements

Make sure, your SQL statements are easy to replay in a generic SQL tool. 

For example, if you write an `UPDATE` or `MERGE` statement with complex inner logic, it is a good idea to put that logic in the FROM-clause instead of the SET clauses.

__Don't__

```
UPDATE [#to_object_address#] SET 
  t.col1 = (SELECT (CASE WHEN ... THEN opt1 ELSE opt2 END) FROM [#to_object_address#] t LEFT JOIN ...),
  t.modified_on = GETDATE() 
WHERE flow_run_uid = [#flow_run_uid#]
```

__Do__

```
UPDATE f SET 
  t.col1 = X.col1,
  t.modified_on = GETDATE() 
FROM (
  SELECT 
      (CASE WHEN ... THEN opt1 ELSE opt2 END) AS col1,
      opt1,
      opt2,
      ...
    FROM 
      [#to_object_address#] t
	  LEFT JOIN ...
	WHERE t.flow_run_uid = [#flow_run_uid#]
  ) AS X
  INNER JOIN [#to_object_address#] t ON X.Id = t.Id
```

This is a lot more text to write, but the inner SELECT can be easily copy-pasted to any SQL tool to visualize the results and to debug it.