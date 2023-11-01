package io.trino.hive.formats.ion;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class IonSerDeProperties {

    public static List<String> HIVE_SERDE_CLASSNAMES = ImmutableList.of("com.amazon.ion.IonHiveSerDe");
}
