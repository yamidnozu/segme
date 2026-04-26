import type { HTMLAttributes, PropsWithChildren } from "react";

type CardProps = PropsWithChildren<HTMLAttributes<HTMLDivElement>>;

export function Card({ className = "", children, ...props }: CardProps) {
  return (
    <div className={`border bg-white text-slate-900 ${className}`.trim()} {...props}>
      {children}
    </div>
  );
}

export function CardContent({ className = "", children, ...props }: CardProps) {
  return (
    <div className={className} {...props}>
      {children}
    </div>
  );
}
