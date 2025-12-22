"use client";
import { useShow } from "@refinedev/core";
import { Show } from "@refinedev/antd";
import SnapshotShowView from "@components/SnapshotList/SnapshotDetail"

export default function SnapshotShowPage() {
  const { query } = useShow<any>();
  const { data, isLoading } = query;

  return (
    <Show isLoading={isLoading}>
        <SnapshotShowView record={data?.data} />
    </Show>
  );
}