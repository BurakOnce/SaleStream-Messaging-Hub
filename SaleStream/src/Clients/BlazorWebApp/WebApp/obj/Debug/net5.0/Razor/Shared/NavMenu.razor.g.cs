#pragma checksum "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\Shared\NavMenu.razor" "{8829d00f-11b8-4213-878b-770e8597ac16}" "d596b4343f6662cf64df634d6004411234c2efd80434416b8240e93f63d0bd9b"
// <auto-generated/>
#pragma warning disable 1591
namespace WebApp.Shared
{
    #line default
    using global::System;
    using global::System.Collections.Generic;
    using global::System.Linq;
    using global::System.Threading.Tasks;
    using global::Microsoft.AspNetCore.Components;
#nullable restore
#line 1 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using System.Net.Http

#nullable disable
    ;
#nullable restore
#line 2 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using System.Net.Http.Json

#nullable disable
    ;
#nullable restore
#line 3 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using Microsoft.AspNetCore.Components.Forms

#nullable disable
    ;
#nullable restore
#line 4 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using Microsoft.AspNetCore.Components.Routing

#nullable disable
    ;
#nullable restore
#line 5 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using Microsoft.AspNetCore.Components.Web

#nullable disable
    ;
#nullable restore
#line 6 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using Microsoft.AspNetCore.Components.Web.Virtualization

#nullable disable
    ;
#nullable restore
#line 7 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using Microsoft.AspNetCore.Components.WebAssembly.Http

#nullable disable
    ;
#nullable restore
#line 8 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using Microsoft.JSInterop

#nullable disable
    ;
#nullable restore
#line 9 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using WebApp

#nullable disable
    ;
#nullable restore
#line 10 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\_Imports.razor"
using WebApp.Shared

#line default
#line hidden
#nullable disable
    ;
    #nullable restore
    public partial class NavMenu : global::Microsoft.AspNetCore.Components.ComponentBase
    #nullable disable
    {
        #pragma warning disable 1998
        protected override void BuildRenderTree(global::Microsoft.AspNetCore.Components.Rendering.RenderTreeBuilder __builder)
        {
            __builder.OpenElement(0, "div");
            __builder.AddAttribute(1, "class", "top-row pl-4 navbar navbar-dark");
            __builder.AddAttribute(2, "b-c2wpa7m53j");
            __builder.AddMarkupContent(3, "<a class=\"navbar-brand\" href b-c2wpa7m53j>WebApp</a>\r\n    ");
            __builder.OpenElement(4, "button");
            __builder.AddAttribute(5, "class", "navbar-toggler");
            __builder.AddAttribute(6, "onclick", global::Microsoft.AspNetCore.Components.EventCallback.Factory.Create<global::Microsoft.AspNetCore.Components.Web.MouseEventArgs>(this, 
#nullable restore
#line 3 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\Shared\NavMenu.razor"
                                             ToggleNavMenu

#line default
#line hidden
#nullable disable
            ));
            __builder.AddAttribute(7, "b-c2wpa7m53j");
            __builder.AddMarkupContent(8, "<span class=\"navbar-toggler-icon\" b-c2wpa7m53j></span>");
            __builder.CloseElement();
            __builder.CloseElement();
            __builder.AddMarkupContent(9, "\r\n\r\n");
            __builder.OpenElement(10, "div");
            __builder.AddAttribute(11, "class", 
#nullable restore
#line 8 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\Shared\NavMenu.razor"
             NavMenuCssClass

#line default
#line hidden
#nullable disable
            );
            __builder.AddAttribute(12, "onclick", global::Microsoft.AspNetCore.Components.EventCallback.Factory.Create<global::Microsoft.AspNetCore.Components.Web.MouseEventArgs>(this, 
#nullable restore
#line 8 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\Shared\NavMenu.razor"
                                        ToggleNavMenu

#line default
#line hidden
#nullable disable
            ));
            __builder.AddAttribute(13, "b-c2wpa7m53j");
            __builder.OpenElement(14, "ul");
            __builder.AddAttribute(15, "class", "nav flex-column");
            __builder.AddAttribute(16, "b-c2wpa7m53j");
            __builder.OpenElement(17, "li");
            __builder.AddAttribute(18, "class", "nav-item px-3");
            __builder.AddAttribute(19, "b-c2wpa7m53j");
            __builder.OpenComponent<global::Microsoft.AspNetCore.Components.Routing.NavLink>(20);
            __builder.AddAttribute(21, "class", (object)("nav-link"));
            __builder.AddAttribute(22, "href", (object)(""));
            __builder.AddAttribute(23, "Match", (object)(global::Microsoft.AspNetCore.Components.CompilerServices.RuntimeHelpers.TypeCheck<global::Microsoft.AspNetCore.Components.Routing.NavLinkMatch>(
#nullable restore
#line 11 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\Shared\NavMenu.razor"
                                                     NavLinkMatch.All

#line default
#line hidden
#nullable disable
            )));
            __builder.AddAttribute(24, "ChildContent", (global::Microsoft.AspNetCore.Components.RenderFragment)((__builder2) => {
                __builder2.AddMarkupContent(25, "<span class=\"oi oi-home\" aria-hidden=\"true\" b-c2wpa7m53j></span> Home\r\n            ");
            }
            ));
            __builder.CloseComponent();
            __builder.CloseElement();
            __builder.AddMarkupContent(26, "\r\n        ");
            __builder.OpenElement(27, "li");
            __builder.AddAttribute(28, "class", "nav-item px-3");
            __builder.AddAttribute(29, "b-c2wpa7m53j");
            __builder.OpenComponent<global::Microsoft.AspNetCore.Components.Routing.NavLink>(30);
            __builder.AddAttribute(31, "class", (object)("nav-link"));
            __builder.AddAttribute(32, "href", (object)("counter"));
            __builder.AddAttribute(33, "ChildContent", (global::Microsoft.AspNetCore.Components.RenderFragment)((__builder2) => {
                __builder2.AddMarkupContent(34, "<span class=\"oi oi-plus\" aria-hidden=\"true\" b-c2wpa7m53j></span> Counter\r\n            ");
            }
            ));
            __builder.CloseComponent();
            __builder.CloseElement();
            __builder.AddMarkupContent(35, "\r\n        ");
            __builder.OpenElement(36, "li");
            __builder.AddAttribute(37, "class", "nav-item px-3");
            __builder.AddAttribute(38, "b-c2wpa7m53j");
            __builder.OpenComponent<global::Microsoft.AspNetCore.Components.Routing.NavLink>(39);
            __builder.AddAttribute(40, "class", (object)("nav-link"));
            __builder.AddAttribute(41, "href", (object)("fetchdata"));
            __builder.AddAttribute(42, "ChildContent", (global::Microsoft.AspNetCore.Components.RenderFragment)((__builder2) => {
                __builder2.AddMarkupContent(43, "<span class=\"oi oi-list-rich\" aria-hidden=\"true\" b-c2wpa7m53j></span> Fetch data\r\n            ");
            }
            ));
            __builder.CloseComponent();
            __builder.CloseElement();
            __builder.CloseElement();
            __builder.CloseElement();
        }
        #pragma warning restore 1998
#nullable restore
#line 28 "C:\Projeler\Asp.Net\SaleStream\src\Clients\BlazorWebApp\WebApp\Shared\NavMenu.razor"
       
    private bool collapseNavMenu = true;

    private string NavMenuCssClass => collapseNavMenu ? "collapse" : null;

    private void ToggleNavMenu()
    {
        collapseNavMenu = !collapseNavMenu;
    }

#line default
#line hidden
#nullable disable

    }
}
#pragma warning restore 1591
