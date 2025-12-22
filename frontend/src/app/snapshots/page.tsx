"use client";

import { List, DateField } from "@refinedev/antd";
import { useTable } from "@refinedev/antd";
import SnapshotList from "@/components/SnapshotList/SnapshotList";

export default function SnapshotListPage() {
  return (
    <List>
      <SnapshotList />
    </List>
  );
}
