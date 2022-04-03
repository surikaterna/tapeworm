export type Callback<T> =
  | ((err: Error, data: undefined) => void)
  | ((err: null, data: T) => void);
