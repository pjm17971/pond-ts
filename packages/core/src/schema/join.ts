import type {
  ColumnDef,
  OptionalizeColumns,
  SeriesSchema,
  ValueColumn,
  ValueColumnsForSchema,
} from './series.js';

export type JoinType = 'inner' | 'left' | 'right' | 'outer';
export type JoinConflictMode = 'error' | 'prefix';

type JoinColumns<
  Left extends readonly ValueColumn[],
  Right extends readonly ValueColumn[],
> = [...OptionalizeColumns<Left>, ...OptionalizeColumns<Right>];

export type JoinSchema<
  Left extends SeriesSchema,
  Right extends SeriesSchema,
> = readonly [
  Left[0],
  ...JoinColumns<ValueColumnsForSchema<Left>, ValueColumnsForSchema<Right>>,
];

type ColumnNamesForSchema<S extends SeriesSchema> =
  ValueColumnsForSchema<S>[number]['name'];
type DuplicateNamesForPair<
  Left extends SeriesSchema,
  Right extends SeriesSchema,
> = Extract<ColumnNamesForSchema<Left>, ColumnNamesForSchema<Right>>;

type PrefixNameIfDuplicate<
  Name extends string,
  Duplicates extends string,
  Prefix extends string,
> = Name extends Duplicates ? `${Prefix}_${Name}` : Name;

type PrefixedOptionalizeColumn<
  Column extends ValueColumn,
  Duplicates extends string,
  Prefix extends string,
> =
  Column extends ColumnDef<infer Name, infer Kind>
    ? ColumnDef<PrefixNameIfDuplicate<Name, Duplicates, Prefix>, Kind> & {
        readonly required: false;
      }
    : never;

type PrefixedOptionalizeColumns<
  Columns extends readonly ValueColumn[],
  Duplicates extends string,
  Prefix extends string,
> = Columns extends readonly [infer Head, ...infer Tail]
  ? Head extends ValueColumn
    ? Tail extends readonly ValueColumn[]
      ? [
          PrefixedOptionalizeColumn<Head, Duplicates, Prefix>,
          ...PrefixedOptionalizeColumns<Tail, Duplicates, Prefix>,
        ]
      : []
    : []
  : [];

export type PrefixedJoinSchema<
  Left extends SeriesSchema,
  Right extends SeriesSchema,
  Prefixes extends readonly [string, string],
> = readonly [
  Left[0],
  ...PrefixedOptionalizeColumns<
    ValueColumnsForSchema<Left>,
    DuplicateNamesForPair<Left, Right>,
    Prefixes[0]
  >,
  ...PrefixedOptionalizeColumns<
    ValueColumnsForSchema<Right>,
    DuplicateNamesForPair<Left, Right>,
    Prefixes[1]
  >,
];

type JoinManySchemaHelper<
  Acc extends SeriesSchema,
  Rest extends readonly SeriesSchema[],
> = Rest extends readonly [infer Head, ...infer Tail]
  ? Head extends SeriesSchema
    ? Tail extends readonly SeriesSchema[]
      ? JoinManySchemaHelper<JoinSchema<Acc, Head>, Tail>
      : never
    : never
  : Acc;

export type JoinManySchema<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
> = Schemas extends readonly [infer Head, ...infer Tail]
  ? Head extends SeriesSchema
    ? Tail extends readonly SeriesSchema[]
      ? JoinManySchemaHelper<Head, Tail>
      : never
    : never
  : never;

type DuplicateNamesAcrossSchemasHelper<
  Schemas extends readonly SeriesSchema[],
  Seen extends string = never,
  Duplicates extends string = never,
> = Schemas extends readonly [infer Head, ...infer Tail]
  ? Head extends SeriesSchema
    ? Tail extends readonly SeriesSchema[]
      ? DuplicateNamesAcrossSchemasHelper<
          Tail,
          Seen | ColumnNamesForSchema<Head>,
          Duplicates | Extract<ColumnNamesForSchema<Head>, Seen>
        >
      : Duplicates
    : never
  : Duplicates;

type DuplicateNamesAcrossSchemas<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
> = DuplicateNamesAcrossSchemasHelper<Schemas>;

type PrefixTupleForSchemas<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
> = {
  [K in keyof Schemas]: string;
};

type PrefixedJoinManyColumns<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
  Prefixes extends PrefixTupleForSchemas<Schemas>,
  Duplicates extends string = DuplicateNamesAcrossSchemas<Schemas>,
> = Schemas extends readonly [infer Head, ...infer Tail]
  ? Prefixes extends readonly [infer PrefixHead, ...infer PrefixTail]
    ? Head extends SeriesSchema
      ? PrefixHead extends string
        ? Tail extends readonly [SeriesSchema, ...SeriesSchema[]]
          ? PrefixTail extends PrefixTupleForSchemas<Tail>
            ? [
                ...PrefixedOptionalizeColumns<
                  ValueColumnsForSchema<Head>,
                  Duplicates,
                  PrefixHead
                >,
                ...PrefixedJoinManyColumns<Tail, PrefixTail, Duplicates>,
              ]
            : never
          : [
              ...PrefixedOptionalizeColumns<
                ValueColumnsForSchema<Head>,
                Duplicates,
                PrefixHead
              >,
            ]
        : never
      : never
    : never
  : [];

export type PrefixedJoinManySchema<
  Schemas extends readonly [SeriesSchema, ...SeriesSchema[]],
  Prefixes extends PrefixTupleForSchemas<Schemas>,
> = readonly [Schemas[0][0], ...PrefixedJoinManyColumns<Schemas, Prefixes>];
