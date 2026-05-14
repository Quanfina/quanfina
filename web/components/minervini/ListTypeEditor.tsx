import { forwardRef, useImperativeHandle, useState } from "react";
import type { ICellEditorParams } from "ag-grid-community";
import { LIST_LABELS } from "@/types/minervini";
import type { ListType } from "@/types/minervini";

const LIST_TYPES: ListType[] = ["watch", "on_deck", "focus", "buy"];

export const ListTypeEditor = forwardRef<unknown, ICellEditorParams>(
  function ListTypeEditor(props, ref) {
    const [value, setValue] = useState<ListType>(props.value as ListType);

    useImperativeHandle(ref, () => ({
      getValue: () => value,
    }));

    function handleChange(e: React.ChangeEvent<HTMLSelectElement>) {
      const next = e.target.value as ListType;
      setValue(next);
      setTimeout(() => props.stopEditing?.(), 0);
    }

    return (
      <select
        value={value}
        onChange={handleChange}
        autoFocus
        className="h-full w-full px-2 text-sm bg-background border-0 outline-none cursor-pointer"
      >
        {LIST_TYPES.map((lt) => (
          <option key={lt} value={lt}>
            {LIST_LABELS[lt]}
          </option>
        ))}
      </select>
    );
  }
);
