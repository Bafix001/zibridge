import { Header } from "@components/header";
import { ThemedLayout } from "@refinedev/antd";
import React from "react";

export default async function Layout({ children }: React.PropsWithChildren) {
  return <ThemedLayout Header={Header}>{children}</ThemedLayout>;
}
