import type { ButtonHTMLAttributes, PropsWithChildren } from "react";

type ButtonVariant = "default" | "outline";

type ButtonProps = PropsWithChildren<
  ButtonHTMLAttributes<HTMLButtonElement> & {
    variant?: ButtonVariant;
  }
>;

const base =
  "inline-flex items-center justify-center whitespace-nowrap text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 disabled:pointer-events-none disabled:opacity-50";

const variants: Record<ButtonVariant, string> = {
  default: "bg-slate-900 text-white hover:bg-slate-800",
  outline: "border border-slate-300 bg-white text-slate-900 hover:bg-slate-100",
};

export function Button({ className = "", variant = "default", children, ...props }: ButtonProps) {
  const classes = `${base} ${variants[variant]} ${className}`.trim();
  return (
    <button className={classes} {...props}>
      {children}
    </button>
  );
}
