"use client";

import {
  DateField,
  List,
  ShowButton,
  useTable,
} from "@refinedev/antd";
import { type BaseRecord } from "@refinedev/core";
import { Space, Table, Tag } from "antd";

export default function SnapshotList() {
  const { tableProps } = useTable({
    syncWithLocation: true,
  });

  return (
    <List>
      <Table {...tableProps} rowKey="id">
        <Table.Column 
          dataIndex="id" 
          title="Snapshot ID"
          sorter
        />
        <Table.Column
          dataIndex="timestamp"
          title="Date"
          render={(value: string) => <DateField value={value} format="DD/MM/YYYY HH:mm" />}
          sorter
        />
        <Table.Column 
          dataIndex="source" 
          title="Source"
          render={(value: string) => (
            <Tag color="blue">{value}</Tag>
          )}
        />
        <Table.Column 
          dataIndex="item_count" 
          title="Objects"
          render={(value: number) => (
            <Tag color="green">{value.toLocaleString()}</Tag>
          )}
        />
        <Table.Column 
          dataIndex="status" 
          title="Status"
          render={(value: string) => {
            const color = value === "completed" ? "green" : "orange";
            return <Tag color={color}>{value}</Tag>;
          }}
        />
        <Table.Column
          title="Actions"
          dataIndex="actions"
          render={(_, record: BaseRecord) => (
            <Space>
              <ShowButton hideText size="small" recordItemId={record.id} />
            </Space>
          )}
        />
      </Table>
    </List>
  );
}